use 5.010;
use strict;
use warnings;
use if $ENV{DEBUG} => 'Debug::Comments';

my $DEFAULT = 'DEFAULT'; # default pool name
my $SIG     = 'CONT';
my $THREADS = threads::posix->can('create') ? 'threads::posix' : 'threads';
my $MAIN    = $THREADS->can('self') && $THREADS->self;

package Thread::Subs;

use threads::shared;
use Scalar::Util qw(looks_like_number);
use Sub::Util qw(set_subname subname);
use Thread::Queue;
use Thread::Semaphore;
use Time::HiRes qw(time);

#our @CARP_NOT; # TODO: tune for effective error messages

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }
sub _bad { _die("Invalid Thread attribute: @_") }
sub _sem { Thread::Semaphore->new(@_) }
sub _queue { Thread::Queue->new }

my %POOL;          # per-pool worker count
my %SHIM;          # per-package shim setting
my %CLIM  :shared; # per-sub concurrency limit Semaphores
my %DEFER :shared; # per-sub queues for concurrency limits
my %QLIM  :shared; # per-sub queue limit Semaphores
my %REQ   :shared; # per-pool request queues
my %SUB;           # all subs Thread::Subs::attr
my %TASK  :shared; # per-thread current sub

my $ENDWAIT = 0;
my $WORKERS = 0;
my $STAGE = 0; # 0: defs, 1: pools, 2: workers, 3: shims, 4: stop

# See start_workers for possible redefinition.
sub _send_callback_signal {
    #@! Sending $SIG pseudo-signal
    $MAIN->kill($SIG);
    return;
}

# Call this in a thread to avoid namespace pollution.
sub _can_tgkill {
    my @x = eval {
        require "syscall.ph";
        return ()
            unless exists(&SYS_gettid)
            and exists(&SYS_tgkill);
        our %Config;
        require "Config.pm";
        import Config '%Config';
        return () unless $Config{sig_name} and $Config{sig_num};
        my @sig = split(' ', $Config{sig_name});
        my @num = split(' ', $Config{sig_num});
        for (0..$#sig) {
            return (&SYS_gettid, &SYS_tgkill, $num[$_])
                if $sig[$_] eq $SIG and defined($num[$_]);
        }
        return ();
    };
    #@! _can_tgkill: @{[$@ ? $@ : "(@x)"]}
    return @x;
}

INIT {
    if ($WORKERS and defined($MAIN) and not $THREADS->tid) {
        #@! Auto-starting workers
        &end_definitions;
        set_pool($DEFAULT => $WORKERS);
        &start_workers;
        &deploy_shims;
    }
}

sub import {
    _die("Import unavailable because end_definitions() has been called")
        if $STAGE > 0;
    my ($class, %arg) = @_;
    my $caller = caller;
    while (my ($n, $v) = each %arg) {
        if ($n eq 'attributes' and $v) {
            no strict 'refs';
            push @{"${caller}::ISA"}, 'Thread::Subs::attributes';
            $SHIM{$caller} = $v ne 'noshim';
        }
        elsif ($n eq 'autostart') {
            _die("Invalid number of workers '$v'")
                if $v and $v =~ /\D/;
            $WORKERS = $v;
        }
        elsif ($n eq 'endwait') {
            _die("Invalid endwait '$v'")
                unless looks_like_number($v) and $v >= 0;
            $ENDWAIT = $v if $v > $ENDWAIT;
        }
        elsif ($n eq 'signal') {
            _die("Invalid signal '$v'")
                if $v and not exists $SIG{$v};
            $SIG = $v || '';
        }
        else { _die("Invalid $class import option '$n'") }
    }
    return;
}

sub _name {
    my ($sub) = @_;
    $sub = subname($sub) if ref $sub;
    no strict 'refs';
    _die("Sub '$sub' does not exist")
        unless exists &{$sub};
    return $sub;
}

sub _define_one {
    _die("BUG: define() called after end_definitions()")
        if $STAGE > 0;
    my ($sub, $prop) = @_;
    $sub = _name($sub);
    my $attr = $SUB{$sub} //= Thread::Subs::attr->new;
    for (keys %$prop) {
        _die("Invalid define option '$_' for $sub")
            unless m/^(?:pool|clim|qlim|void|shim)$/;
        my $val = $prop->{$_};
        _die("Option '$_' must be numeric in define for $sub")
            if /^[cq]lim$/ && $val && $val =~ /\D/;
        $attr->$_($val);
    }
    if ($attr->clim) { $CLIM{$sub} = _sem($attr->clim); $DEFER{$sub} = _queue() }
    else             { delete $CLIM{$sub}; delete $DEFER{$sub} }
    if ($attr->qlim) { $QLIM{$sub} = _sem($attr->qlim - 1) }
    else             { delete $QLIM{$sub} }
    return;
}

sub define {
    if (@_ == 1 and ref($_[0]) eq 'HASH') {
        my ($hash) = @_;
        _define_one($_, $hash->{$_})
            for keys %$hash;
    }
    elsif (@_ >  1 and ref($_[1]) eq 'HASH') {
        _define_one(shift, shift)
            while @_;
    }
    else  {
        my ($sub, %opts) = @_;
        _define_one($sub, \%opts);
    }
    return;
}

sub _attribute {
    my ($class, $sub) = @_;
    return 1 unless /^Thread(?:\(|$)/;
    $sub = _name($sub);
    my @attr = /^Thread\((.+)\)$/ ? ($1 =~ m/[^, ]+/g) : ();
    #@! Handling attributes for $sub
    my %attr = (
        clim => sub { m/^=(\d+)$/ ? $1 : _bad("clim$_") },
        pool => sub { m/^=(\w+)$/ ? $1 : _bad("pool$_") },
        qlim => sub { m/^=(\d+)$/ ? $1 : _bad("qlim$_") },
        void => sub { m/^$/       ? 1  : _bad("'void' takes no value") },
        );
    my %opt = (shim => $SHIM{$class} ? 1 : 0);
    for (@attr) {
        my ($name, $val) = /^(\w+)(=.+)?$/;
        _bad("'$_' is unrecognised")
            unless $name && exists($attr{$name});
        _bad("multiple '$name' definitions")
            if exists $opt{$name};
        $opt{$name} = $attr{$name}->() for $val//'';
    }
    if ($opt{pool}) {
        if    ($opt{pool} eq 'SUB') { $opt{pool} = $sub   }
        elsif ($opt{pool} eq 'PKG') { $opt{pool} = $class }
    }
    _define_one($sub, \%opt);
    return;
}

sub MODIFY_CODE_ATTRIBUTES {
    my ($class, $code, @attr) = @_;
    return grep { _attribute($class, $code) } @attr;
}
*Thread::Subs::attributes::MODIFY_CODE_ATTRIBUTES = \&MODIFY_CODE_ATTRIBUTES;

sub end_definitions {
    if ($STAGE == 0) {
        $STAGE = 1;
        for (values %SUB) {
            my $p = $_->pool;
            my $c = $_->clim;
            $POOL{$p} //= 1;
            $POOL{$p} = $c if $c > $POOL{$p};
        }
    }
    return wantarray ? %POOL : scalar(keys %POOL);
}

sub set_pool {
    &end_definitions if $STAGE == 0;
    _die("BUG: set_pool() called when workers already started")
        if $STAGE > 1;
    while (@_) {
        my $pool = shift;
        _die("No subs use worker pool '$pool'")
            unless exists $POOL{$pool};
        my $count = shift;
        _die("Invalid worker count '$count' for pool '$pool'")
            if $count =~ /\D/ or $count < 1;
        $POOL{$pool} = $count;
    }
    return wantarray ? %POOL : scalar(keys %POOL);
}

sub _be_worker {
    my ($pool) = @_;
    my $tid = $THREADS->tid;
    #@! Worker $tid spawned for $pool pool
    while (defined(my $work = $REQ{$pool}->dequeue)) {
        my ($result, $sub, @arg) = @$work;
        my $clim = $CLIM{$sub};
        if ($clim) {
            lock($clim); # exclusive on $clim and $DEFER{$sub}
            unless ($clim->down_nb) {
                #@! Request for $sub deferred due to concurrency limit
                $DEFER{$sub}->enqueue($work);
                next;
            }
        }
        #@! Worker $tid started $sub
        $TASK{"$tid-$pool"} = $sub;
        my $qlim = $QLIM{$sub};
        while ($work) {
            undef $work;
            $qlim->up if $qlim;
            if ($result) {
                no strict 'refs';
                my @res = eval { $sub->(@arg) };
                if (my $ex = $@) { $result->croak($ex) }
                else             { $result->send(@res) }
            }
            else {
                no strict 'refs';
                eval { $sub->(@arg) };
            }
            #@! Worker $tid executed $sub
            if ($clim) {
                lock($clim); # exclusive on $clim and $DEFER{$sub}
                $work = $DEFER{$sub}->dequeue_nb;
                if ($work) { ($result, undef, @arg) = @$work }
                else       { $clim->up }
            }
        }
        #@! Worker $tid finished $sub
        $TASK{"$tid-$pool"} = '';
    }
    #@! Worker $tid exits
    return;
}

sub start_workers {
    _die("Can't start workers: threads not available")
        unless defined $MAIN;
    &end_definitions if $STAGE == 0;
    _die("BUG: workers already started")
        if $STAGE > 1;
    $STAGE = 2;
    if ($SIG and $THREADS eq 'threads') {
        #@! Lame thread signals detected; testing for tgkill() capability
        my ($caps) = $THREADS->create(\&_can_tgkill);
        if (my ($gettid, $tgkill, $signum) = map { $_ + 0 } $caps->join) {
            #@! Support for tgkill() detected (syscall $tgkill)
            $! = 0;
            my $tid = syscall($gettid);
            _die("gettid syscall failed: $!") if $!;
            no warnings 'redefine';
            *_send_callback_signal = sub {
                #@! Sending signal $signum ($SIG) to $$/$tid via tgkill
                syscall($tgkill, $$, $tid, $signum);
            };
        }
    }
    for my $pool (keys %POOL) {
        my $count = $POOL{$pool};
        #@! Starting $pool worker pool ($count)
        $REQ{$pool} = _queue();
        for (1..$count) {
            my $tid = $THREADS->create(\&_be_worker, $pool)->tid;
            $TASK{"$tid-$pool"} = '';
        }
    }
    #@! @{[$SIG ? "Using $SIG signal" : "No handler"]} for callbacks
    $SIG{$SIG} = \&Thread::Subs::result::run_callback_queue
        if $SIG;
    return wantarray ? %POOL : scalar(keys %POOL);
}

sub shim {
    _die("BUG: shim requested before workers started")
        if $STAGE < 2;
    my ($sub) = @_;
    $sub = _name($sub);
    my $attr = $SUB{$sub} or _die("BUG: '$sub' is not a threaded sub");
    my $pool = $attr->pool;
    my $void = $attr->void;
    my $qlim = $QLIM{$sub};
    return sub {
        #@! Requesting $sub pool=$pool void=@{[$void?'yes':'no']} qlim=@{[$qlim?$$qlim+1:'no']}
        my $res = $void ? undef : Thread::Subs::result->new;
        $REQ{$pool}->enqueue(shared_clone([$res, $sub, @_]));
        $qlim->down if $qlim; # can block
        return $void ? () : $res;
    };
}

sub deploy_shims {
    _die("BUG: attempt to deploy shims at wrong stage (STAGE=$STAGE)")
        unless $STAGE == 2;
    _die("BUG: attempted to deploy shims in a thread")
        if $THREADS->tid;
    $STAGE = 3;
    for (grep { $SUB{$_}->shim } keys %SUB) {
        no strict 'refs';
        no warnings 'redefine';
        *{$_} = set_subname($_, shim($_));
        #@! Deployed shim for $_
    }
    return;
}

sub stop_workers {
    if ($STAGE < 4) {
        $STAGE = 4;
        for (keys %REQ) {
            #@! Shutting down $_ worker pool
            $REQ{$_}->end;
        }
    }
    return;
}

sub running_workers {
    my @thr;
    for (keys %TASK) {
        if (my $t = $THREADS->object(/^(\d+)/)) {
            if    ($t->is_joinable) { $t->join; delete $TASK{$_} }
            elsif ($t->is_running)  { push @thr, $t }
        }
        else { delete $TASK{$_} } # detached thread terminated?
    }
    return @thr;
}

sub stop_and_wait {
    #@! Stopping and waiting for workers
    &stop_workers;
    select(undef, undef, undef, 0.05) while &running_workers;
    #@! All worker threads joined
    &Thread::Subs::result::run_callback_queue;
    return;
}

sub current_tasks { &running_workers; return %TASK }

END {
    #@! END: Shutting down workers
    &stop_workers;
    my $lim = $ENDWAIT + time();
    while (&running_workers) {
        if (time < $lim) { select(undef, undef, undef, 0.05) }
        else {
            #@! END: Detaching remaining workers
            $_->detach for &running_workers;
            return;
        }
    }
    #@! END: All threads joined
}


package Thread::Subs::attr;
sub new  { bless [] }
sub pool { @_ == 1 ? $_[0][0] || $DEFAULT : do { $_[0][0] = $_[1]; $_[0] } }
sub clim { @_ == 1 ? $_[0][1] || 0        : do { $_[0][1] = $_[1]; $_[0] } }
sub qlim { @_ == 1 ? $_[0][2] || 0        : do { $_[0][2] = $_[1]; $_[0] } }
sub void { @_ == 1 ? !!$_[0][3]           : do { $_[0][3] = $_[1]; $_[0] } }
sub shim { @_ == 1 ? !!$_[0][4]           : do { $_[0][4] = $_[1]; $_[0] } }


package Thread::Subs::result;

use threads::shared;

my %CB;
my @CBQ :shared; # call-back queue (ready)
my $CBF :shared; # call-back flag (do not signal when true)

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }
sub _push_cbq  { lock(@CBQ); push @CBQ, @_ }
sub _flush_cbq { lock(@CBQ); my @q = @CBQ; @CBQ = (); return @q }
END { %CB = () } # no callbacks post-exit

sub new {
    my ($class) = @_;
    my @self :shared = 0;
    return bless(\@self, ref($class)||$class);
}

sub _callback {
    my ($self) = @_;
    my $id = is_shared($self);
    if ($id && $CB{$id}) {
        _die("BUG: attempt to invoke callback on unready result")
            unless $self->[0];
        eval { (delete $CB{$id})->($self) };
    }
    return;
}

sub run_callback_queue {
    _die("BUG: result callbacks must be invoked in the main thread")
        if $THREADS->tid;
    $CBF = 1; # Suppress signals
    #@! Invoking callbacks
    my $n = 0;
    while (@CBQ) { for (&_flush_cbq) { $n++; $_->_callback } }
    $CBF = 0; # Enable signals, then catch residuals
    for (&_flush_cbq ) { $n++; $_->_callback }
    #@! Callback processing complete ($n)
    return;
}

sub cb {
    _die("BUG: result cb method only available in the main thread")
        if $THREADS->tid;
    my ($self, $cb) = @_;
    my $id = is_shared($self)
        or _die("BUG: result object is not shared");
    if (@_ > 1) {
        delete $CB{$id};
        $cb->($self) if $cb and do {
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
    my $cb;
    {
        lock($self);
        $cb = !$self->[0] && $self->[1];
        @$self = @$args;
        cond_broadcast($self);
    }
    if ($cb) {
        if ($THREADS->tid) {
            # Wrong thread: enqueue and maybe signal
            _push_cbq($self);
            &Thread::Subs::_send_callback_signal
                if $SIG && !$CBF;
        }
        else { $self->_callback }
    }
    return $self;
}

sub send  { shift()->_set( 1, @_) }
sub croak { shift()->_set(-1, @_) }

sub _wait {
    my ($self) = @_;
    unless ($self->[0]) {
        lock($self);
        cond_wait(@$self) until $self->[0];
    }
    return $self;
}

sub data {
    my ($self) = @_;
    $self->_wait unless $self->[0];
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
__END__

=head1 NAME

Thread::Subs - Execute selected subs concurrently in worker threads

=head1 SYNOPSIS

TODO (when remaining docs are finalised)

=head1 DESCRIPTION

This module provides a relatively simple way to execute subroutines
concurrently in separate threads.  All "simplicity" is relative where
parallelism is concerned, but this module manages the creation and
termination of worker threads, provides attributes whereby a sub can
be marked as threaded, allows limits to be placed on concurrency and
outstanding requests, and provides an asynchronous results interface.
The net effect is that you can simply declare a sub as "Thread" and
then call it asynchronously, so long as the data in and out can be
shared using L<threads::shared>.

Note that this documentation is not a tutorial on threading or even on
Perl threads in particular.  It aims to be as accessable as possible,
but some understanding of the Perl L<threads> and L<threads::shared>
mechanisms are assumed.  That may be quite a lot to assume, because
that documentation itself discourages its own use.  Rest assured that
the aim of this module is to make threads far more practical.

There are quite a few moving parts behind the scenes which make this
all work.  Here's the big-picture view of what's going on.

=head2 Attributes

Perl has an L<attributes> mechanism which allows the language to be
extended in various ways.  This module uses that mechanism to add a
"Thread" attribute to sub declarations.  This allows the user to
declare specific subs as threaded and express some parameters such as
concurrency limits.  These attributes can also be applied through
explicit function calls, but attributes allow the properties to be
expressed as part of the static sub declaration.

Here is a basic example.

    sub foo :Thread(qlim=10 clim=1 void) { ... }

This declares that sub foo() can be called in a thread: "qlim=10"
means there can be up to ten such calls waiting to execute, "clim=1"
means only one instance of the sub can execute concurrently, and
"void" means it does not return a result.  These parameters and others
are described in more detail later.

=head2 Workers

The threads which execute the subs are "workers", potentially divided
into named "pools" associated with particular subs.  In the simplest
case, all workers are part of the "DEFAULT" pool.  Workers are spawned
early in the process lifecycle and persist until shut down.  You can
decide how many workers and pools you want.

Each worker pool is associated with a L<Thread::Queue> object into
which requests are enqueued; workers take from the head of this queue
when ready.  Insertion into the queue is subject to an optional "qlim"
limit which can cause the request to block.  Execution is also subject
to optional concurrency limits, and requests will be placed into a
per-sub "deferred" queue if that limit is reached, to be handled as
soon as a worker currently processing such a sub is ready.

=head2 Results

Because the results of threaded subs only become available some time
later, the value returned immediately is a "result" object with an API
very similar to an L<AnyEvent> condition variable.  This object also
provides methods to convert the result into other popular async result
methods such as L<Future> and L<Mojo::Promise>.

The final value can be obtained from a "result" object in two ways:
blocking wait, or callback.  In the case of a blocking wait, the
C<recv()> operation blocks using L<threads::shared> cond_wait() until
the worker signals completion.  In the callback case, a callback is
associated with a request: it is called immediately if the result is
already available, or from a signal handler when it becomes available.

Note that the "result" object is capable of conveying either a list of
returned data or an exception condition.  The execution context for a
threaded sub is always a list, but if the sub raises an exception it
will be caught and then re-thrown when the result is evaluated.

=head2 Shims

A "shim" is a function or library which transparently intercepts API
calls and changes the arguments passed, handles the operation itself
or redirects the operation elsewhere.  A related term is "wrapper",
which is simply a thin layer of additional logic around pre-existing
functionality.  This module turns ordinary subs into threaded subs
using this kind of mechanism: the logic which converts an ordinary
function call into a complex, asynchronous, queued dispatch process to
a worker pool with concurrency limits is simply called a "shim" here.

The process can't be completely transparent because calls change from
blocking/synchronous to nonblocking/asynchronous, and it's very hard
to hide such a fundamental change.  Aside from the "result" object, as
discussed in the previous section, however, the change is surprisingly
transparent.  Once the properties of all threaded subs are declared
and the worker threads start up, the original subs can be replaced (in
the main thread only) with shims; this allows them to be called in the
same way as normal subs, modulo the fact that they return a "result"
object immediately instead of blocking until they return data.  The
code inside a threaded sub need not do anything special at all: data
in and out is handled in the usual way.

Replacing the original subs with shims is not always the best option,
but the shim itself is simply a CODE reference (a closure) which can
be generated for any given threaded sub.  You can use this value in
all the usual ways, as you prefer, and opt out of auto-shims if they
don't help.  Note, however, that CODE references are not portable
between threads: a thread must generate its own shims, and only the
main thread offers automated shim deployment.

=head1 IMPORTING

First, note that you should "use threads" before using this module or
any other module which uses this module if you intend to make use of
its functionality.  Using this module does not oblige you to use
threads, but it is effectively a no-op unless you do.  You may want to
tune the thread stack size while you're at it.

The import method, normally called implicitly at "use", expects a list
of name-value pairs.  Unrecognised names are fatal; the valid names
and associated value restrictions are as follows.

=head2 attributes

A true value causes the "Thread" sub attribute to be recognised in the
importing package.  Note that all "Thread" subs in the package are
auto-shimmed unless the value is "noshim", specifically.  Details of
the attribute syntax are given in the L<ATTRIBUTES> section.

This feature is enabled on a per-package basis by adding a sub-package
to the caller's @ISA array containing the C<MODIFY_CODE_ATTRIBUTES>
method which implements sub attribute processing.  This only works if
that method is not defined locally or inherited elsewhere, which is
nearly always the case, but you'll need to make special arrangements
if using more than one provider of sub attributes.

=head2 autostart

Takes an integer value greater than zero, or false (the default).  If
true, this automates the worker start-up process.  The value is the
number of threads to start for the DEFAULT pool.  Other pools get one
worker or the largest "clim" value associated with a sub in that pool,
if any are specified.  After all workers are started, deploy_shims()
is called.  This all happens in an INIT block, so threaded subs will
be available by the time your main code starts.

This approach is convenient for the simpler cases where attributes are
sufficient to define your workforce.  I suggest you use an environment
variable with fallback to a constant for the number of workers.

=head2 endwait

Takes a numeric value of zero or more; default zero.  When the process
exits, some worker threads may still be running, either because the
work takes a while or because there are still requests in the queue.
This value gives the number of seconds to wait in the END state before
giving up and detaching them.  The workers will stop naturally if they
complete all remaining work before this time limit.

You may want to set this to a nonzero value if your threads are
potentially doing something you'd rather not interrupt, but the
trade-off is that process exit may be delayed.

=head2 signal

Takes a signal name (%SIG key) or a false value; default 'CONT'.  The
callback mechanism relies on worker threads sending a signal to the
main thread.  The callback is then executed in the main thread in the
context of this signal handler.  If you set this to a false value,
then no signal handler is installed and callbacks won't work unless
you provide an alternative mechanism (see L</"run_callback_queue">).

The signal handler is installed right after the workers start if true.
An exception is raised if it's not valid.  See also L</"SIGNALS">.

=head1 ATTRIBUTES

Where the module is imported with a true "attributes" parameter or
some other technique is used to invoke the C<MODIFY_CODE_ATTRIBUTES>
method from your package, subs can declare a "Thread" attribute with
the following syntax.

All parameters are optional; where any parameters are present, they
must be enclosed in parentheses, as in "Thread(void)".  Parameters are
separated by spaces and/or commas when more than one is present, as in
"Thread(clim=1, void)".  If the parameter is associated with a value,
the name must be followed immediately by an equals sign and then the
value, as in "Thread(pool=foo)".

Unrecognised parameter names produce a compile-time failure.  Valid
names and their associated values (if any) are as follows.

=head2 clim

Concurrency limit: an upper limit on how many worker threads may
execute this sub simultaneously; also used as a hint to suggest a size
for the worker pool, as the pool would need to be at least this large
for the value to be meaningful as a limit.  The associated value must
be an integer of one or more.  Where absent, no limit is applied other
than the natural limit of the number of running workers.  A common
case is "clim=1", which allows the sub to be concurrent with the main
thread and other subs, but not with itself.

=head2 pool

The worker pool name which executes the sub, which is "DEFAULT" unless
specified otherwise.  The special name "SUB" is replaced by the full
name of the sub itself (e.g. "main::foo") to facilitate worker pools
dedicated to a particular sub.  Similarly, "PKG" is replaced by the
package name in which the sub resides, facilitating a package-specific
pool.  Names must otherwise be at least one character long and consist
of alphanumerics and underscore - a limitation imposed to keep the
attribute syntax simple, not a limitation on pool names as such.

=head2 qlim

Queue limit: an upper limit on the number of requests for a particular
sub which can be outstanding, with no assigned worker.  This value
must be an integer of at least one.  Where absent, there is no limit,
which means requests never block, but the request queue can grow
indefinitely.  It's generally better to manage request limits in some
other way, particularly if you are also using an event loop of some
kind, but this limit can be convenient in simple cases.

Note the following particulars of the blocking mechanism.  First, the
request is enqueued I<before> the limit is checked; any blocking
occurs I<afterwards>, delaying the function's return by waiting for a
worker to remove at least one request from the queue if the limit has
been reached.  A L<Thread::Semaphore> object is used.

The case of "qlim=1" thus has rather special semantics: it will always
hit the limit when it adds the request to the queue, so it won't
return until a worker takes the task from the queue, meaning that the
worker has I<started> working on the request.  This semi-synchronous
behaviour may occasionally be quite useful.  In general, however, such
a small limit is unnecessarily restrictive.

=head2 void

This parameter takes no value and designates a sub which returns no
value.  When called as a threaded sub, it will return undef/empty
immediately rather then return a "result" object.  That's one less
thing to worry about, but it leaves you with no way to tell when the
sub finishes.  As such, you may prefer to omit this option even if the
sub returns nothing just so you can tell when it's finished, or else
you run the risk of exiting your main process before it's done.  As an
alternative, you could grant it a grace period with the L</"endwait">
import option.

=head1 FUNCTIONS

The module is primarily driven by functions, but also has a "result"
object to convey the results of subs executed in worker threads.  This
section deals with the functions; see L</"RESULTS"> for the object.

No functions are imported and the import semantics do not support it.
Functions should be called with their fully qualified names.  Note
also that these functions are highly dependent on execution order.
The overall process is divided up into stages, and each function is
valid only in particular stages, as outlined below.

=over 4

=item *

Stage zero is available immediately after the module is imported, and
is the stage where sub attributes are defined, either by the attribute
mechanism or calls to C<define()>.

=item *

Stage one, triggered by C<end_definitions()>, closes off definitions
and evaluates worker pools implied by those definitions.  The pools
can be resized with C<set_pool()> in this stage.

=item *

Stage two, triggered by C<start_workers()>, starts up the worker
pools, at which point it becomes possible to generate shims and
actually call the subs.

=item *

Stage three, triggered by C<deploy_shims()>, replaces the threaded
subs in the main thread with shims (unless disabled).  This is the
normal operation stage.

=item *

Stage four, triggered by C<stop_workers()>, commences shutdown by
closing off the request queues and terminating idle workers.

=back

The functions are presented below in the natural calling order, along
with their associated restrictions.  Violation of the calling order
requirements will result in an exception.  Most of these functions are
unnecessary if you use sub attributes and specify the L</"autostart">
import option, but some flexibility is sacrificed in that approach.

=head2 define

    Thread::Subs::define(\%defs);              # single hashref
    Thread::Subs::define($sub, \%params, ...); # sub-hashref pairs
    Thread::Subs::define($sub, %params);       # sub, name-value pairs

This is a more flexible alternative to the L<ATTRIBUTES> mechanism,
allowing the properties of threaded subs to be specified.  It is not
mutually exclusive with attributes, though for the sake of clarity I
suggest that you don't override attribute definitions.  It is only
available in stage zero.

The calling semantics permit one or many subs to be defined in a
single call, but the all-in-one hashref approach can only identify
functions by name because hash keys are necessarily strings.  The
other approaches permit $sub to be either a string or a reference to
the sub, but see L</"Quirks of Sub Names"> for caveats about using
references.  Anonymous subs are not allowed because CODE references
are not a thread-sharable data type: a request to execute a sub must
refer to the sub by name.  Work around this by assigning the sub to a
glob, thus giving it a name.

The %parameters are the same as the L</"ATTRIBUTES"> parameters with a
couple of exceptions arising from the difference between attribute
strings and name-value pairs.  First, the "pool" name can be any
string; "SUB" and "PKG" are not special cases: use the literal sub or
package name if you want to achieve the same effect.  Second, "void"
takes a boolean value, normally 1 since the default is false.  Third,
there is a "shim" parameter, also boolean and default false, which
declares whether the L</"deploy_shims"> function should redefine it.
This is implicitly true for attribute-defined functions unless the
import option "attributes => 'noshim'" was specified.

=head2 end_definitions

    %pool = Thread::Subs::end_definitions();

This function is only available in stage zero.  It marks the end of
sub definitions and calculates base worker pool sizes from those
definitions.  All pools will have at least one worker, but the number
will be increased to match the largest "clim" value in the pool, if
any.  On return, stage one has commenced and no further calls to
C<define()> are permitted.

In a list context, a list of name-value pairs is returned, where the
names are all the pool names and the values are the base worker count.
In a scalar context, the number of pools is returned.  Unless you need
these values for pool planning, calling this function is optional
because C<set_pool()> and C<start_workers()> call it on demand.

Note that the end of definitions will also prohibit any further use of
the "import" method, in case you were thinking of calling it outside
the context of "use" for any reason.

=head2 set_pool

    %pool = Thread::Subs::set_pool($pool, $count, ...);

This function is permitted in stages zero and one; if called in stage
zero it calls C<end_definitions()> on your behalf to commence stage
one.  It allows the number of workers per pool to be adjusted from the
base values, as returned by C<end_definitions()>.  It's not possible
to create or delete pools this way: all threaded subs are associated
with a pool at this point, and all such pools must have at least one
worker, so all you can do is adjust the numbers.  An exception is
raised if any $pool argument does not match an existing name, or if
any $count is not an integer greater than zero.

The return value is as per C<end_definitions()>, post-adjustment.

=head2 start_workers

This function is permitted in stages zero and one; if called in stage
zero it calls C<end_definitions()> on your behalf to commence stage
one.  It then spawns all the threads in the worker pools, creates the
associated L<Thread::Queue> objects, and installs the signal handler
for callbacks (unless it is disabled).  When it returns, stage two has
commenced.  The function takes no arguments and returns the same pool
size data as C<set_pool()> and C<end_definitions()>, except that it's
final this time and reflects what's actually running.

You will need to call this function unless you are using the import
option L</"autostart">.  This function will fail if L<threads> was not
loaded, of course.

=head2 shim

    $code = Thread::Subs::shim($sub);

This function is only available in stage two and up.  It returns a
$code ref which can be used to call $sub in a worker thread.  The $sub
can be given as a name or as a reference, but it must have "Thread"
attributes or have been the subject of an earlier C<define()> call.
See L</"Quirks of Sub Names"> for caveats relating to the use of sub
references.

The specific parameters which affect the shim are "pool", which tells
it where to send the request; "void", which tells it whether to return
a "result" object; and "qlim", which tells it to potentially block
before returning.  The "shim" option has no effect on this function:
that option only alters the behaviour of C<deploy_shims()>.

=head2 deploy_shims

This function is only available in stage two.  It takes no arguments,
returns nothing, and can only be called from the main thread.  When it
returns, stage three has commenced.  It replaces all the threaded subs
bearing the "shim" option with shims, meaning that subsequent calls to
those subs will use the asynchronous interface and run in a worker.
This change only affects the main thread and any threads you spawn
subsequently: the workers continue to see the original sub.

This replacement has pros and cons.  See the earlier discussion of
L</"Shims"> for details and alternatives.  You are under no strict
obligation to use this function, but it may be tidier than the
alternative, which involves more explicit use of C<shim()>.

=head2 stop_workers

This function takes no arguments and returns nothing.  It is valid at
any stage, and when it returns, stage four has commenced.  It shuts
down the queues so that no further subs can be requested: any requests
already in the queue will still be processed, and worker threads will
exit when there is no further work to do.  Attempting to use a shim in
stage four will raise an immediate exception.

Calling this function is optional as it is always called during END
processing, with possible additional delay if the L</"endwait"> import
option was defined.  The function effectively becomes a no-op once
called, and it is not possible to restart the workers once stopped.

=head2 stop_and_wait

As per L</"stop_workers">, but does not return until all worker
threads have exited and all callbacks have executed.  This is very
convenient for simple scripts, but it can hang on a stuck worker.

=head2 running_workers

    @threads = Thread::Subs::running_workers();

This function, primarily intended for internal use, returns a list of
worker L<threads> objects which are still running.  It also "joins"
any workers which have ended.  May be called at any time.

A possible use for this is to detect dead workers.  It's important for
workers to keep running, so simple exceptions will not take them down,
but there are edge cases beyond control which can theoretically cause
a worker thread to die.  If you have a long-running process, you may
want to do an occasional worker head-count with this function and bail
out if any have gone missing.

=head2 current_tasks

    %tasks = Thread::Subs::current_tasks();

Provides a snapshot of the current state of workers in the form of ID
and sub-name pairs.  The ID is a combination of the thread ID and the
pool name ("$tid-$pool").  Idle workers have an empty string for the
sub name.  May be called at any time.

=head1 RESULTS

The "result" sub-object (Thread::Subs::result) is returned by the shim
code which requests that a worker execute a sub unless that sub has
been defined as "void".  The interface is very similar to "condition
variables" in L<AnyEvent> with some minor tweaks and caveats.

It's unlikely that you'll want to create any of these objects, so the
documentation starts with the methods of most interest given that you
already have one.

=head2 recv

    @data = $result->recv;

This is a blocking receive operation: it will block until a result has
been sent, then either return that @data or raise an exception if the
result was a failure.  Returns C<$data[0]> in a scalar context.

=head2 data

    @data = $result->data;

As per C<recv()>, but returns the exception string as data in the case
of failure rather than raising an exception.  See also L</"failed">.
This has no equivalent in L<AnyEvent>.

=head2 cb

    $code = $result->cb;
    $code = $result->cb($code);

Gets and optionally sets the callback for the $result.  This can only
be done from the main thread because while it's possible in principle
to have callbacks to any thread, it would be very complex to implement
and use, so support is limited to the simplest case.

You can only set one callback: it will be called immediately if the
$result is already available, or from a signal handler when it becomes
available.  This module reduces the use of signals by not sending them
while the main thread is actively processing the callback queue, but
one should still keep the contents of a callback to the same basics
which are suitable in a signal handler.

An explicit undef argument cancels the callback, and the callback is
also removed on execution.  The callback is passed the $result as an
argument with the promise that it is now ready, such that C<recv()>
and C<data()> won't block.  Exceptions in callback code are absorbed
and ignored, as are returned values.

Note that all outstanding callbacks are cancelled when the process
reaches the END state.  Avoid calling C<exit()> before callbacks are
complete if that's undesirable.

=head2 ready

Boolean: true if the result is ready, false if it isn't.

=head2 failed

Boolean: true if the result is ready and it is a failure (generated by
C<croak()>).  This has no equivalent in L<AnyEvent>.  The typical use
case is in callback code like the following.

    my $cb = sub {
        my ($result) = @_;
        my @data = $result->data;
        if ($result->failed) { do_fail_thing(@data) }
        else { do_success_thing(@data) }
    };

=head2 run_callback_queue

This is a function which takes no arguments, but it can be invoked as
a method if desired.  It is normally installed as the signal handler
specified by the L</"signal"> import parameter, but you'll need to
make other arrangements if you've disabled that for some reason.  When
called (from the main thread only), it executes callbacks on all ready
results associated with a callback, and clears the queue.

=head2 Async Adaptors

There are three methods designed to adapt this async result object to
other similar systems.  All of these methods rely on the callback
mechanism, so they are mutually exclusive with each other per object
and will replace any existing callback.

=head3 ae_cv

This requires L<AnyEvent> to be loaded and returns a real L<AnyEvent>
condition variable.  This is preferable if you are using L<AnyEvent>,
because calling C<recv()> on it will run the event loop.

=head3 mojo_promise

This requires L<Mojo::Promise> to be loaded and returns an object of
that type which will C<resolve()> or C<reject()> in accordance with
the result object.

=head3 future

This requires L<Future> to be loaded and returns an object of that
type which will be C<done()> or C<fail()> in accordance with the
result object.

=head2 Other Methods

The following methods are primarily intended for internal use.  They
correspond to the same methods for L<AnyEvent> condition variables.

=head3 new

Class method: returns a new object in the "pending" (not ready) state.

=head3 send

The object becomes "ready" and the data passed as arguments become the
result data.  Returns self.

=head3 croak

The object becomes "ready" and "failed"; the data passed becomes the
exception reason.  Returns self.

=head1 SIGNALS

As mentioned in the documentation for the L</"signal"> import argument
and the L</"run_callback_queue"> function, result callbacks require
the use of a signal to execute callbacks in the main thread.  This is
the 'CONT' signal unless specified otherwise.

'CONT' is a slightly cheeky choice of signal as the default: given the
standard meaning of 'CONT' (resume if stopped), it would normally be
pointless for a process to send itself such a signal because if it can
send itself a signal then it's not stopped.  Even so, 'CONT' is a
signal which can be handled like any other, and we are technically
telling something to continue by using it.  You can still suspend the
process with 'STOP'; the callback queue will be checked on resume due
to the 'CONT' signal, but this is harmless.

The primary advantage of 'CONT' is simply that nothing else is likely
to use it.  If it's too exotic for your tastes, select a conventional
user signal instead.  Just ensure that nothing else installs a %SIG
handler for the chosen signal, or callbacks will cease to work.

Note also that Perl's support for thread-specific signals is poor.
The signals built into the threads module are not real OS signals and
do not interrupt system calls, which may prevent timely resolution of
callbacks in event-loop systems.  This module uses the Linux-specific
C<tgkill()> syscall instead of C<< threads->kill >> if it can detect
support for it, but falls back to native pseudo-signals if not.  For
platforms other than Linux, try the CPAN module L<threads::posix>
which adds real per-thread OS signal capabilities via the pthreads
library.  This module uses L<threads::posix> instead of L<threads> if
it's already loaded.

If you really can't use the signal at all, you can disable it with a
false value at import, but callbacks won't work except to the extent
that you call C<Thread::Subs::result::run_callback_queue()> yourself.

=head1 NOTES

=head2 Use Cases

Dispatching subs to separate threads carries a fair bit of overhead
compared to normal in-thread calls, but there some compelling use
cases which make the cost worth it.  These scenarios represent good
opportunities to improve throughput.

Ultimately there is no substitute for empirical testing when trying
to determine whether threaded subs improve your performance or not,
but these guidelines will help you to find the low-hanging fruit.

=head3 CPU-Intensive Work

The first case is CPU-intensive work which can be parallelised for
speed.  Multi-core CPUs are common now, so parallelism can pay big
dividends.  CPU-bound work should generally be applied to a single
pool which is slightly smaller than your total CPU count, the intent
being to ensure there is spare CPU for other activities.

A more sophisticated approach is to adjust thread priority, lowering
the priority of CPU-bound code, but this is not easy to do portably.
If you happen to be using Linux, the L<POSIX> C<nice()> function can
be used to temporarily lower the priority of a thread, even though the
POSIX standard says it should operate on the whole process.

=head3 Resource Pools

A second use case is exemplified by database interaction.  A common
pattern is that of a web-based application with information in a
database.  On the one side there are many concurrent clients, and on
the other there are limited database connection resources.  A thread
pool offers a good solution to this mismatch, since it allows a large
number of event-driven clients in the main thread access to a limited
pool of database workers, each with its own connection.  Idle database
connections are minimised.

The fact that each thread has its own copy of the global space can be
quite useful in this context.  Each of the DB worker threads can do
its own lazy-open on the database, caching the handle while valid,
just as one might in a single-threaded application.

=head3 The Power of One

Lastly, do not overlook the utility of dedicated specialist workers.
At first glance, "clim=1" may seem like it defeats the whole purpose
of threads, but it actually has a lot to offer.  Parallelism can be
much easier to manage in such a localised manner.  A simple example is
the idea of a log-writing thread: you likely want to emit log messages
at various points in your code without delaying the primary task, and
this is a good case for void threaded subs executed by a specialist.

Specialists in a dedicated pool of one have the additional advantage
of being able to maintain state.  It's possible for multiple threads
to share state, but it requires careful avoidance of race conditions
and other such issues.  It's immensely simpler with only a single
thread and no possibility of conflict: you get the benefits of some
parallelism at almost no complexity cost.

=head3 Bad Ideas

Very short-running subs called with high frequency are the worst kind
of thing to delegate to workers.  You not only pay a significant cost
in overhead for the call, but will probably pay even more because of
contention for the associated locks.

Having said that, the frequency must be very high and the execution
time must be very short for it to be a bad idea.  If a sub takes one
millisecond to execute, the theoretical maximum synchronous call rate
is one thousand per second.  This is still well within the bounds
where parallelism can increase the throughput.

=head2 Quirks of Sub Names

The dispatch mechanism passes a fully qualified sub name to the
worker, which then invokes the sub using a symbolic reference.  As
such, subs must appear in the global symbol table to be executable in
this way.  Depending on how the sub was created, however, its "name"
may or may not match its global symbol entry.  Subs declared using the
"sub" keyword and a name will be fine, but if you create a sub by
assigning a CODE reference to a glob, the "name" is a property of the
CODE reference, not the glob.  A lot of importing happens this way.

What this means, in simple terms, is that using a CODE reference in a
C<define()> call to select the sub might not work, even if it's a
reference to the global symbol like C<\&foo>.  If it was created using
an alias, like C<*foo = \&bar;>, the "name" will be the name of the
original sub, which may or may not work.  Using a plain string is the
safer approach.  The usual argument against it is that there is no
compile-time checking of the name, but run-time checking is performed
by C<define()> fairly early in the process lifecycle, and that's
almost as good.

=head2 Limitations and Workarounds

Thread subs can't receive or return the more esoteric data types such
as globs or code refs.  The glob limitation affects filehandles, so
you'll need to make special arrangements to deal with them.

The simplest approach is to pass filenames instead of handles, though
this may result in excessive opening and closing if done naively.  A
good cheat is to have a dedicated worker assigned to a set of subs
that deal with a particular file.  The worker is then able to store
related state in global variables without difficulty.  A dedicated
package suits this pattern well.

You can also pass C<fileno()> file descriptors rather than file names
if they are real OS-based files.

=head2 Objects

Direct support for objects can be hit and miss.  You can certainly
design an object to operate with threaded methods: it just needs to
constrain itself to the limits of L<threads::shared> data and not
store object data outside the object.  Then, so long as all the
methods called on the object are shimmed, the object is threaded.  All
the internal method-to-method calls still use the original synchronous
interface, so the object does not need to be explicitly thread-aware.

If an object meets the data requirements but you don't want to shim
its methods directly, write threaded sub wrappers around the part of
the API you want to use asynchronously.  These functions, being new,
won't affect any existing code.

=head2 Threads Calling Threads

It's possible for worker threads to call other threaded subs, subject
to some limitations.  Most of the time it's simply best to call other
subs the old fashioned synchronous way, but there are reasonable cases
where you may prefer an asynchronous call, particularly a void one.

The first major rule is that worker threads can only call threaded
subs via a closure returned from C<shim()>.  The C<deploy_shims()>
operation happens after worker threads start, so workers always see
the original global subs, not the shimmed replacements.

The second major rule is that worker threads can only obtain results
via the blocking C<recv()> or C<data()> methods, not callbacks.  Void
subs are perfectly fine, of course, but callbacks are strictly limited
to the main thread.

Lastly, watch out for potential deadlock situations.  A worker that
blocks waiting for other workers is a potential source of deadlock,
and it's on you to ensure the potential can't become reality.

=head1 SEE ALSO

TODO

=head1 LICENSE AND COPYRIGHT

This software is Copyright (c) 2025 by Brett Watson.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
