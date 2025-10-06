use 5.010;
use strict;
use warnings;
use threads;
use if $ENV{DEBUG} => 'Debug::Comments';

package Thread::Subs;

use threads::shared;
use Sub::Util qw(subname);
use Thread::Queue;
use Thread::Semaphore;

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }
sub _bad { _die("Invalid Thread attribute: @_") }
sub _sem { Thread::Semaphore->new(@_) }
sub _queue { Thread::Queue->new }

my $REQUESTS = _queue();
my $SIG      = 'USR2';
my $WORKERS  = 10;
#@! Using $WORKERS workers, $SIG signal

my %QLIM  :shared; # per-sub queue limit Semaphores
my %DEFER :shared; # per-sub queues for concurrency limits
my %SUB;           # all subs "returns nothing" flag
my %TASK  :shared; # per-thread current sub
my %TLIM  :shared; # per-sub thread (concurrency) limit Semaphores

my @CBQ   :shared; # call-back queue

=pod

Attributes:

queue - limit on number of queued requests; default no limit, min 1
pool  - max number of workers; default automatic, min 1
void  - if specified, does not return a value

sub foo :Thread(queue=1 pool=1 void)

=cut

sub attribute {
    my ($sub) = @_;
    $sub = subname($sub) if ref($sub);
    return 1 unless /^Thread(?:\(|$)/;
    _die("Anonymous subs cannot be threaded")
        if $sub eq '__ANON__';
    my @attr = /^Thread\((.+)\)$/ ? ($1 =~ m/[^, ]+/g) : ();
    #@! Handling attributes for $sub
    $SUB{$sub} = 0; # 'void' overrides this
    my $pool  = sub { $TLIM{$sub} = _sem(@_); $DEFER{$sub} = _queue() };
    my $queue = sub { $QLIM{$sub} = _sem(@_) };
    my %attr = (
        pool  => sub { m/^=(\d+)$/ && $1 > 0 ? $pool->($1)  : _bad("pool$_")  },
        queue => sub { m/^=(\d+)$/ && $1 > 0 ? $queue->($1) : _bad("queue$_") },
        void  => sub { m/^$/ ? ($SUB{$sub} = 1) : _bad("'void' takes no value") },
        );
    my %opt;
    for (@attr) {
        my ($name, $val) = /^(\w+)(=.+)?$/;
        _bad("'$_' is unrecognised")
            unless $name && exists($attr{$name});
        _bad("multiple '$name' definitions")
            if $opt{$name}++;
        $attr{$name}->() for $val//'';
    }
    return;
}

sub MODIFY_CODE_ATTRIBUTES {
    my ($class, $code, @attr) = @_;
    return grep { attribute($code) } @attr;
}
*Thread::Subs::attributes::MODIFY_CODE_ATTRIBUTES = \&MODIFY_CODE_ATTRIBUTES;

sub import {
    my $class = shift;
    my $caller = caller;
    #@! import $class into $caller
    no strict 'refs';
    push @{"${caller}::ISA"}, 'Thread::Subs::attributes';
    return;
}

sub worker {
    my $tid = threads->tid;
    #@! Worker $tid spawned
    $SIG{TERM} = sub { threads->exit };
    while (defined(my $work = $REQUESTS->dequeue)) {
        my ($result, $sub, @arg) = @$work;
        my $tlim = $TLIM{$sub};
        if ($tlim) {
            lock($tlim); # exclusive on $tlim and $DEFER{$sub}
            unless ($tlim->down_nb) {
                #@! Request for $sub deferred due to pool limit
                $DEFER{$sub}->enqueue($work);
                next;
            }
        }
        #@! Worker $tid started $sub
        $TASK{$tid} = $sub;
        my $qlim = $QLIM{$sub};
        while ($work) {
            undef $work;
            $qlim->up if $qlim;
            if ($result) {
                no strict 'refs';
                my @res = eval { $sub->(@arg) };
                if ($@) { $result->croak($@)  }
                else    { $result->send(@res) }
            }
            else {
                no strict 'refs';
                eval { $sub->(@arg) };
            }
            #@! Worker $tid executed $sub
            if ($tlim) {
                lock($tlim); # exclusive on $tlim and $DEFER{$sub}
                $work = $DEFER{$sub}->dequeue_nb;
                if ($work) { ($result, undef, @arg) = @$work }
                else       { $tlim->up }
            }
        }
        #@! Worker $tid finished $sub
        $TASK{$tid} = '';
    }
    #@! Worker $tid exits
    return;
}

sub _gen_call {
    my ($sub, $void, $qlim) = @_;
    #@! Making shim for $sub
    return sub {
        #@! Intercept $sub void=@{[$void?'yes':'no']} qlim=@{[$qlim?$$qlim:'no']}
        my @data :shared;
        $qlim->down if $qlim;
        my $res = $void ? undef : Thread::Subs::result->new;
        @data = ($res, $sub, @_);
        _die("Thread::Subs workers have terminated")
            unless $REQUESTS;
        $REQUESTS->enqueue(\@data);
        return $void ? () : $res;
    };
}

INIT {
    #@! INIT: @{[keys %SUB]}
    $WORKERS //= 10;
    for (map { threads->create(\&worker)->tid } 1..$WORKERS) {
        $TASK{$_} = '';
    }
    for (keys %SUB) {
        no strict 'refs';
        no warnings 'redefine';
        *{$_} = _gen_call($_, $SUB{$_}, $QLIM{$_});
    }
    $SIG{$SIG} = sub {
        #@! SIG$SIG caught
        my @jobs;
        { lock(@CBQ); @jobs = @CBQ; @CBQ = () }
        $_->_invoke for @jobs;
        #@! Signal handler complete
    };
}

sub running_workers {
    my @thr;
    for (keys %TASK) {
        if (my $t = threads->object($_)) {
            if    ($t->is_joinable) { $t->join; delete $TASK{$_} }
            elsif ($t->is_running)  { push @thr, $t }
        }
        else { delete $TASK{$_} } # detached thread terminated?
    }
    return @thr;
}

sub terminate {
    $REQUESTS->end if $REQUESTS;
    undef $REQUESTS;
    return;
}

END {
    #@! END: Shutting down workers
    $SIG{$SIG} = 'IGNORE';
    $REQUESTS->end if $REQUESTS;
    my @thr = &running_workers;
    $_->kill('TERM') for @thr;
    $_->join for @thr;
    #@! END: All threads joined
}


package Thread::Subs::result;

use threads::shared;

my %CB;

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }

sub new {
    my ($class) = @_;
    my @self :shared = 0;
    return bless(\@self, ref($class)||$class);
}

sub _invoke {
    _die("BUG: result callbacks must be invoked in the main thread")
        if threads->tid;
    my ($self) = @_;
    my $id = is_shared($self)
        or _die("BUG: result object is not shared");
    if (exists $CB{$id}) {
        _die("BUG: attempt to invoke callback on unready result")
            unless $self->[0];
        (delete $CB{$id})->($self);
    }
    return $self;
}

sub cb {
    _die("BUG: result cb method only available in the main thread")
        if threads->tid;
    my ($self, $cb) = @_;
    my $id = is_shared($self)
        or _die("BUG: result object is not shared");
    if (@_ > 1) {
        $cb->($self) if do {
            lock($self);
            $self->[0] ? 1 : do { $CB{$id} = $cb; $self->[1] = 1; 0 };
        };
    }
    return $CB{$id};
}

sub ready  { $_[0][0] }
sub failed { $_[0][0] < 0 }

sub _set {
    my $self = shift;
    my $args = shared_clone([@_]);
    my $sig;
    {
        lock($self);
        _die("Result already set") if $self->[0];
        $sig = $self->[1];
        @$self = @$args;
        cond_broadcast($self);
    }
    if ($sig) {
        lock(@CBQ);
        kill $SIG, $$ unless @CBQ;
        push @CBQ, $self;
    }
    return $self;
}

sub send  { shift()->_set( 1, @_) }
sub croak { shift()->_set(-1, @_) }

sub await {
    my ($self) = @_;
    unless ($self->[0]) {
        lock($self);
        cond_wait(@$self) until $self->[0];
    }
    return $self;
}

sub data {
    my ($self) = @_;
    $self->await unless $self->[0];
    my (undef, @data) = @$self;
    return wantarray ? @data : $data[0];
}

sub recv {
    my ($self) = @_;
    my @data = $self->data;
    _die(@data) if $self->failed;
    return wantarray ? @data : $data[0];
}

sub ae_cv {
    my ($self) = @_;
    my $cv = AE::cv();
    my $cb = sub {
        my @data = $_[0]->data;
        if ($_[0]->failed) { $cv->croak(@data) }
        else { $cv->send(@data) }
        return;
    };
    $self->cb($cb);
    return $cv;
}

sub mojo_promise {
    my ($self) = @_;
    my $p = Mojo::Promise->new;
    my $cb = sub {
        my @data = $_[0]->data;
        if ($_[0]->failed) { $p->reject(@data) }
        else { $p->resolve(@data) }
        return;
    };
    $self->cb($cb);
    return $p;
}

sub future {
    my ($self) = @_;
    my $f = Future->new;
    my $cb = sub {
        my @data = $_[0]->data;
        if ($_[0]->failed) { $f->fail(@data) }
        else { $f->done(@data) }
        return;
    };
    $self->cb($cb);
    return $f;
}

sub DESTROY {
    if (my $id = is_shared($_[0])) { delete $CB{$id} }
}

1;
