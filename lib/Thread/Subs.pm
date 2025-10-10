use 5.010;
use strict;
use warnings;
use if $ENV{DEBUG} => 'Debug::Comments';

my $DEFAULT = 'DEFAULT';
my $SIG     = 'USR2';

package Thread::Subs;

use threads::shared;
use Scalar::Util qw(looks_like_number);
use Sub::Util qw(subname);
use Thread::Queue;
use Thread::Semaphore;
use Time::HiRes qw(sleep time);

our @CARP_NOT;

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }
sub _bad { _die("Invalid Thread attribute: @_") }
sub _sem { Thread::Semaphore->new(@_) }
sub _queue { Thread::Queue->new }

my %SHIM;          # per-package shim setting
my %CLIM  :shared; # per-sub concurrency limit Semaphores
my %DEFER :shared; # per-sub queues for concurrency limits
my %QLIM  :shared; # per-sub queue limit Semaphores
my %REQ   :shared; # per-pool request queues
my %SUB;           # all subs Thread::Subs::attr
my %TASK  :shared; # per-thread current sub

my $ENDWAIT = 0;
my $WORKERS = 0;
my $STAGE = 0; # 0: definitions, 1: start workers, 2: deploy shims, 3: stop workers

INIT {
    if (is_shared(%REQ)) {
        #@! INIT: @{[$SIG ? "Using $SIG signal" : "No handler"]} for callbacks
        $SIG{$SIG} = \&Thread::Subs::result::run_callback_queue
            if $SIG;
        if ($WORKERS) {
            my %pool = ($DEFAULT => $WORKERS);
            for (values %SUB) {
                my $p = $_->pool;
                next if $p eq $DEFAULT;
                my $c = $_->clim;
                $pool{$p} //= 1;
                $pool{$p} = $c if $c > $pool{$p};
            }
            start_workers(%pool);
            deploy_shims();
        }
    }
}

sub import {
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
    _die("Anonymous subs cannot be threaded")
        if $sub eq '__ANON__';
    no strict 'refs';
    _die("Sub '$sub' does not exist")
        unless exists &{$sub};
    return $sub;
}

sub _define_one {
    _die("BUG: attempt to define sub properties after worker threads started")
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
    $opt{pool} = $sub if $opt{pool} && $opt{pool} eq 'SUB';
    _define_one($sub, \%opt);
    return;
}

sub MODIFY_CODE_ATTRIBUTES {
    my ($class, $code, @attr) = @_;
    return grep { _attribute($class, $code) } @attr;
}
*Thread::Subs::attributes::MODIFY_CODE_ATTRIBUTES = \&MODIFY_CODE_ATTRIBUTES;

sub _be_worker {
    my ($pool) = @_;
    my $tid = threads->tid;
    #@! Worker $tid spawned for $pool pool
    $SIG{TERM} = sub { threads->exit };
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
        $TASK{$tid} = $sub;
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
        $TASK{$tid} = '';
    }
    #@! Worker $tid exits
    return;
}

sub start_workers {
    _die("BUG: attempt to start workers after shims deployed")
        if $STAGE > 1;
    $STAGE = 1;
    unshift @_, $DEFAULT if @_ == 1;
    while (@_) {
        my $pool = shift;
        _die("No subs use worker pool '$pool'")
            unless $pool eq $DEFAULT or grep { $pool eq $_->pool } values %SUB;
        _die("'$pool' worker pool is already running")
            if $REQ{$pool};
        my $count = shift;
        _die("Invalid worker count '$count' for pool '$pool'")
            if $count =~ /\D/ or $count < 1;
        #@! Starting $pool worker pool ($count)
        $REQ{$pool} = _queue();
        $TASK{threads->create(\&_be_worker, $pool)->tid} = ''
            for 1..$count;
    }
    return;
}

sub shim {
    _die("BUG: shim requested before workers started")
        if $STAGE < 1;
    my ($sub) = @_;
    $sub = _name($sub);
    my $attr = $SUB{$sub} //= Thread::Subs::attr->new;
    my $pool = $attr->pool;
    my $void = $attr->void;
    my $qlim = $QLIM{$sub};
    return sub {
        #@! Requesting $sub pool=$pool void=@{[$void?'yes':'no']} qlim=@{[$qlim?$$qlim+1:'no']}
        my @data :shared;
        my $res = $void ? undef : Thread::Subs::result->new;
        @data = ($res, $sub, @_);
        _die("$sub unavailable: Thread::Subs pool '$pool' not started")
            unless $REQ{$pool};
        $REQ{$pool}->enqueue(\@data);
        $qlim->down if $qlim; # can block
        return $void ? () : $res;
    };
}

sub deploy_shims {
    _die("BUG: attempt to deploy shims inappropriately (STAGE=$STAGE)")
        unless $STAGE == 1;
    $STAGE = 2;
    for (grep { $SUB{$_}->shim } keys %SUB) {
        no strict 'refs';
        no warnings 'redefine';
        *{$_} = shim($_);
        #@! Deployed shim for $_
    }
}

sub stop_workers {
    $STAGE = 3;
    @_ = keys %REQ unless @_;
    for (@_) {
        _die("'$_' worker pool is not active")
            unless $REQ{$_};
        #@! Shutting down $_ worker pool
        $REQ{$_}->end;
    }
    return;
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

sub current_tasks { &running_workers; return %TASK }

END {
    #@! END: Shutting down workers
    $SIG{$SIG} = 'IGNORE';
    $_->end for values %REQ;
    my $lim = $ENDWAIT + time();
    while (&running_workers) {
        if (time() < $lim) { sleep(0.1) }
        else {
            #@! END: Terminating remaining workers
            my @thr = &running_workers;
            $_->kill('TERM') for @thr;
            $_->join for @thr;
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

sub _die { exists(&Carp::croak) ? goto &Carp::croak : die "@_\n" }

sub new {
    my ($class) = @_;
    my @self :shared = 0;
    return bless(\@self, ref($class)||$class);
}

sub run_callback_queue {
    _die("BUG: result callbacks must be invoked in the main thread")
        if threads->tid;
    #@! Invoking callbacks
    my @jobs;
    { lock(@CBQ); @jobs = @CBQ; @CBQ = () }
    for (@jobs) {
        my $id = is_shared($_)
            or _die("BUG: result object is not shared");
        if (exists $CB{$id}) {
            _die("BUG: attempt to invoke callback on unready result")
                unless $_->[0];
            (delete $CB{$id})->($_);
        }
    }
    #@! Callback processing complete
    return;
}

sub cb {
    _die("BUG: result cb method only available in the main thread")
        if threads->tid;
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
    my $sig;
    {
        lock($self);
        $sig = !$self->[0] && $self->[1];
        @$self = @$args;
        cond_broadcast($self);
    }
    if ($sig) {
        lock(@CBQ);
        kill $SIG, $$
            if $SIG && @CBQ == 0;
        push @CBQ, $self;
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

There are quite a few moving parts behind the scenes which make this
all work.  Here's the big-picture view of what's going on.

=head2 Attributes

Perl has an L<attributes> mechanism which allows the language to be
extended in various ways.  This module uses that mechanism to add a
"Thread" attribute to sub declarations.  This allows the user to
declare specific subs as threaded and express some parameters such as
concurrency limits.  These attributes can also be applied through
explicit function calls, but attributes allow the properties to be
expressed as part of the sub declaration.

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

Values can be obtained from a result object two ways: blocking wait,
or callback.  In the case of a blocking wait, the "recv" operation
blocks using L<threads::shared> cond_wait() until the worker signals
completion.  In the callback case, a callback is associated with a
request: it is called immediately if the result is already available,
or from a signal handler when it becomes available.

=head2 Shims

The module provides a "shim" mechanism whereby threaded subs can be
replaced (in the main thread only) with a function that puts a request
for that sub in the appropriate queue and returns a "result" object.
This means that a call to a threaded sub can look just like a normal
call except for the "result" object being returned.  Threaded subs
don't have to do anything special either: they are subject to limits
imposed by Perl threads, but they receive and return results in the
usual way.

The "shim" mechanism can also be used to generate a CODE reference
interface to the threaded sub.

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
shimmed unless this true value is "noshim", specifically.  Details of
the attribute syntax are given in the L<ATTRIBUTES> section.

This feature is enabled on a per-package basis by adding a sub-package
to the caller's @ISA array containing the C<MODIFY_CODE_ATTRIBUTES>
method which implements sub attribute processing.  This only works if
that method is not defined locally or inherited elsewhere, which is
nearly always the case, but you'll need to make special arrangements
if using more than one attribute-provider.

=head2 autostart

Takes an integer value greater than zero, or false (the default).  If
true, this automates the worker start-up process.  The value is the
number of threads to start for the DEFAULT pool.  Other pools get one
thread or the largest "clim" value associated with a sub in that pool,
if any are specified.  After all workers are started, deploy_shims()
is called.  This all happens in an INIT block, so threaded subs will
be available by the time your main code starts.

This approach is convenient for the simpler cases where attributes are
sufficient to define your workforce.  I suggest you use an environment
variable with fallback to a constant for the number of workers.

=head2 endwait

Takes a numeric value of zero or more; default zero.  When the process
exits, threads may still be running.  This value gives the number of
seconds to wait before interrupting the threads with a TERM signal to
shut them down.  You may want to set this to a nonzero value if your
threads are potentially doing something you'd rather not interrupt,
but the trade-off is that process exit may be delayed.

=head2 signal

Takes a signal name (%SIG key) or a false value; default 'USR2'.  The
callback mechanism relies on worker threads sending an OS signal to
the main thread.  The callback is then executed in the main thread in
the context of this signal handler.  If you set this to a false value,
then no signal handler is installed and callbacks won't work unless
you provide an alternative mechanism (see L</"run_callback_queue">).

The signal handler is installed during the INIT phase if true.  An
exception will be raised if it's not a valid signal name.

=head1 ATTRIBUTES

Where the module is imported with a true "attributes" parameter or
some other technique is used to invoke the MODIFY_CODE_ATTRIBUTES
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
execute this sub simultaneously.  The associated value must be an
integer of one or more.  Where absent, no limit is applied other than
the natural limit of the number of running workers.  A common case is
"clim=1", which allows the sub to be concurrent with the main thread
but not with itself.

=head2 pool

The worker pool name which executes the sub, which is "DEFAULT" unless
specified otherwise.  The special name "SUB" is replaced by the full
name of the sub itself (e.g. "main::foo") to facilitate worker pools
dedicated to a particular sub.  Names must otherwise be at least one
character long and consist of alnum+underscore.

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
occurs I<afterwards>.  The case of "clim=1" thus has rather specific
semantics: it will always hit the limit when it adds the request to
the queue, and it won't return until after a worker has started
working on the request.  Blocking is handled by L<Thread::Semaphore>.

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

The module is primarily driven by functions, but also has a "results"
object to convey the results of subs executed in worker threads.  This
section deals with the functions; see L</"RESULTS"> for the object.

No functions are imported and the import semantics do not support it.
Functions should be called with their fully qualified names.  In the
interests of brevity, the "Thread::Subs" package name is omitted here.

Note also that these functions are highly dependent on the order of
execution.  When worker threads are spawned, for example, they work on
the function definitions in effect at the time.  In an attempt to keep
things relatively sane, this order is enforced with exceptions, such
that some functions become unavailable once others are called.  The
functions are presented below in the natural calling order.

=head2 define

    define(\%definitions);           # single hashref
    define($sub, \%parameters, ...); # sub-hashref pairs
    define($sub, %parameters);       # sub and name-value pairs

This is a more flexible alternative to the L<ATTRIBUTES> mechanism,
allowing the properties of threaded subs to be specified.  It is not
mutually exclusive with attributes, though for the sake of clarity I
suggest that you don't override attribute definitions.

The calling semantics permit one or many subs to be defined in a
single call, but the all-in-one hashref approach can only identify
functions by name because hash keys are necessarily strings.  The
other approaches permit $sub to be either a string or a reference to
the sub, but anonymous subs are not allowed as references.

The %parameters are the same as the L</"ATTRIBUTES"> parameters with a
couple of exceptions.  First, the "pool" name can be any string and
"SUB" is not a special case: use the actual sub name if you want to
achieve the same effect as "SUB".  Second, "void" takes a boolean
value, normally 1 since the default is false.  Third, there is a
"shim" parameter, also boolean and default false, which declares
whether the deploy_shims() function should redefine it.  This is
implicitly true for attribute-defined functions unless the import
option "attributes => 'noshim'" was specified.

=head2 start_workers

    start_workers($count);
    start_workers($pool, $count, ...);

If you don't use the "autostart" option at import, you will need to
start worker pools using this function.  The single-argument version
starts $count workers in the "DEFAULT" pool; the other variant has
$pool-$count pairs.

The define() function becomes unavailable once this is called.  The
start_workers() function can be called multiple times, but it is an
error for the same $pool to be started more than once, and it's an
error to start a pool (other than "DEFAULT") not used by any subs.

=head2 shim

    $code = shim($sub);

Returns a $code ref which can be used to call $sub in a worker thread.
This is based on the define() parameters in effect at the time, or the
defaults if nothing has been defined.  The $sub can be given as a name
or as a reference, but it must exist.  The specific parameters which
affect the shim are "pool", which tells it where to send the request;
"void", which tells it whether to return a "result" object; and
"qlim", which tells it to potentially block before returning.

The "shim" option has no effect on this: that option only alters the
behaviour of deploy_shims().  This function can only be called after
workers have been started, and the $code it returns has the potential
to deadlock if it has a "qlim" restriction and the associated worker
pool isn't running.

=head2 deploy_shims

This function takes no arguments, returns nothing, requires that
workers have been started, prevents any further workers from starting,
and can only be called once.  Once called, any sub defined with the
"shim" property is replaced by a shim which invokes it in a thread.
Because workers get their own copy of the environment when they spawn,
no workers see this change: only the main thread (and any threads you
spawn subsequently) see the change.

This replacement has pros and cons.  So long as you're only ever
calling the function via its shim, the only surprising aspect is the
fact that it returns an async "result" object instead of direct
results.  Beyond that, the intent is clear.  You'll need to be careful
if you call the sub recursively or from another threaded sub, however:
in both these cases, the original synchronous interface is active.

If the replacement mechanism becomes confusing rather than clarifying,
consider using the shim() function to define new "_threaded" variants
of the subs, or just store the closures in variables and call the subs
that way.  If you want the option of calling the function both ways
from the main thread, you can't deploy shims.

=head2 stop_workers

    stop_workers(@pools);

Signals to the specified worker @pools that no more requests will be
sent, and they should shut down if idle.  If @pools is empty, all
pools are stopped.  It's a fatal error to request a non-running pool
be shut down, and it's a fatal error to use a shim once the associated
worker pool has stopped.

It's not mandatory to call this function: it will be called during END
processing with possible additional delay if the "endwait" import
option was defined.

=head2 running_workers

    @threads = running_workers();

This function, primarily intended for internal use, returns a list of
worker L<threads> objects which are still running.  It also "joins"
any workers which have ended.  May be called at any time.

=head2 current_tasks

    %tasks = current_tasks();

Provides a snapshot of the current state of workers in the form of
thread-id/sub-name pairs.  Idle workers have an empty string for the
sub name.  May be called at any time.

=head1 RESULT

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
result was a failure.  Returns $data[0] in a scalar context.

=head2 data

    @data = $result->data;

As per recv(), but returns the exception string as data in the case of
failure rather than raising an exception.  See also failed().  This is
not available in L<AnyEvent>.

=head2 cb

    $code = $result->cb;
    $code = $result->cb($code);

Gets and optionally sets the callback for the $result.  This can only
be done from the main thread: CODE references are not portable across
threads.  You can only set one callback: it will be called immediately
if the $result is already available, or from a signal handler when it
becomes available.  An explicit undef argument cancels the callback,
and the callback is also removed on execution.  The callback is passed
the $result as an argument with the promise that it is now ready, such
that the recv() and data() methods won't block.

=head2 ready

Boolean: true if the result is ready.

=head2 failed

Boolean: true if the result is ready and it is a failure.  This is not
available in L<AnyEvent>.  The typical use case is in callback code
like the following.

    my $cb = sub {
        my ($result) = @_;
        my @data = $result->data;
        if ($result->failed) { do_fail_thing(@data) }
        else { do_success_thing(@data) }
    };

=head2 run_callback_queue

This is a function which takes no arguments, so it can be invoked as a
method if desired.  It is normally called from the signal handler
specified by the L</"signal"> import parameter, but you'll need to
make other arrangements if you've disabled it for some reason.  When
called (from the main thread only), it invokes callbacks on all ready
results associated with a callback, and clears the queue.

=head2 Async Adaptors

There are three methods designed to adapt this async result object to
other similar systems.  All of these methods rely on the callback
mechanism, so they are mutually exclusive with each other per object
and will replace any existing callback.

=head3 ae_cv

This requires L<AnyEvent> to be loaded and returns a real L<AnyEvent>
condition variable.  This is preferable if you are using L<AnyEvent>,
because calling recv() on it will run the event loop.

=head3 mojo_promise

This requires L<Mojo::Promise> to be loaded and returns an object of
that type which will resolve() or reject() in accordance with the
result object.

=head3 future

This requires L<Future> to be loaded and returns an object of that
type which will be done() or fail() in accordance with the result
object.

=head2 Other Methods

These methods are primarily intended for internal use.  They
correspond to the same methods for L<AnyEvent> condition variables.

=head3 new

Class method: returns a new object in the "pending" (not ready) state.

=head3 send

The object becomes "ready" and the data passed as arguments become the
result data.  Returns self.

=head3 croak

The object becomes "ready" and "failed"; the data passed becomes the
exception reason.  Returns self.

=head1 NOTES

=head2 Use Cases

Dispatching subs to separate threads carries a fair bit of overhead
compared to normal in-thread calls, but there are at least two use
cases which make the cost worth it.

The first case is CPU-intensive work which can be parallelised for
speed.  Multi-core CPUs are common now, so parallelism can pay big
dividends.  CPU-bound work should generally be applied to a single
pool which is slightly smaller than your total CPU count.  A more
sophisticated approach is to adjust thread priority, lowering the
priority of CPU-bound code, but this is not easy to do portably.

The other case involves multi-step operations like database activities
which are more I/O bound, but much easier to write with blocking
semantics than async coding techniques.  Write them the simple way and
give them a pool of threads instead of driving yourself mad with event
loop logic.  As a bonus, you can have a pool of active DB connections.

=head2 Limitations and Workarounds

Thread subs can't receive or return the more esoteric data types such
as globs or code refs.  The glob limitation affects filehandles, so
you'll need to make special arrangements to deal with them.

The simplest approach is to pass filenames instead of handles, though
this may result in excessive opening and closing if done naively.  A
good cheat is to have a single worker assigned to a set of subs that
deal with a particular file.  The single worker is then able to store
related state in global variables without difficulty.  A dedicated
package suits this pattern well.

You can also pass fileno() file descriptors rather than file names if
they are real OS-based files.

The fact that each thread has its own copy of the global space can be
quite useful.  If you have a set of DB worker threads, for example,
and they do a lazy-open on the database, caching the handle while
valid, then each such thread will generate its own DB connection
handle.

=head2 Objects

Direct support for objects can be hit and miss.  You can certainly
design an object to operate with threaded methods: it just needs to
constrain itself to the limits of L<threads::shared> data and not
store object data outside the object.  Then, so long as all the
methods called on the object are shimmed, the object is threaded.

If an object meets the data requirements but you don't want to shim
its methods, write threaded sub wrappers around the part of the object
API you want to use.

