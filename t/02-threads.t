#!perl
use 5.010;
use strict;
use warnings;
BEGIN { eval("use threads") or $::ERR = $@ }
use threads::shared;
use Test::More;
BEGIN {
    unless (threads->can('create')) {
        diag("threads failed: $::ERR") if $::ERR;
        BAIL_OUT("All further testing requires threads");
    }
}
use Thread::Subs;
use Time::HiRes qw(time);

sub nap { select(undef, undef, undef, $_[0] * 0.01); return @_ }

sub test  :Thread         { &nap }
sub dies  :Thread         { nap(5); die "@_\n" }
sub clim1 :Thread(clim=1) { &nap }
sub qlim1 :Thread(qlim=1) { &nap }

{
    package Foo;
    use threads::shared;
    use Thread::Subs;
    sub new { shared_clone(bless []) }
    sub give :Thread(clim=1 pool=PKG) { my $self = shift; push @$self, @_; return $self }
    sub take :Thread(clim=1 pool=PKG) { return shift @{$_[0]} }
}

sub busy_workers {
    my %t = &Thread::Subs::current_tasks;
    return scalar grep { $_ } values(%t)
}

sub all_idle_ok {
    for (1..5) { last unless &busy_workers; nap(1) }
    cmp_ok(&busy_workers, '==', 0, "All workers idle");
}

# Some tests rely on DEFAULT pool having 10 workers
is(scalar(Thread::Subs::startup(10)), 2, "Started two pools");

my $ERR = '';
$SIG{CONT} = do {
    my $sig = $SIG{CONT};
    sub { eval { &$sig }; $ERR .= $@ if $@ }
};
my $WARN = '';
$SIG{__WARN__} = sub { $WARN .= "@_" };

my (@r, $x);

@r = map { test($_) } 0..9;
$x = join('-', map { $_->recv } @r);
is($x, '0-1-2-3-4-5-6-7-8-9', "Blocking recv");

&all_idle_ok;

$x = '';
@r = map { test(9 - $_) } 0..9;
$_->cb(sub { $x .= $_[0]->recv })
    for @r;
eval { $_->recv for @r };
ok(!$@, "No exceptions");
is($x, '0123456789', "Callbacks");

&all_idle_ok;

ok($x = dies('foo'), "Called sub with exception");
ok(!$x->ready, "Not ready yet");
is($x->data, "foo\n", "Exception string returned");
ok($x->failed, "Has failed");
eval { $x->recv };
ok($@, "Recv raises exception");

&all_idle_ok;

$x = '';
for my $n (3,2)   { clim1($n)->cb(sub { $x .= "s$n" }) } # sequential
for my $n (1,2,4) { test($n) ->cb(sub { $x .= "p$n" }) } # parallel
test(6)->recv; # delay
is($x, 'p1p2s3p4s2', "Expected order of completion");

&all_idle_ok;

$x = time + 0.02;
test(2) for 0..9; # all workers busy next 20ms
qlim1(0); # should pass
cmp_ok(time, '<', $x, "Not blocked by queue limit");
qlim1(0); # should block
cmp_ok(time, '>', $x, "Blocked by queue limit");

is($WARN, '', "No warnings");
dies("void");
$x = dies("scalar");
dies("ignored")->data; # forces wait
&all_idle_ok;
like($ERR, qr/void$/, "Void context exception produces exception");
eval { $x->fatal };
like($@, qr/scalar$/, "Fatal method produces exception");
$x->warn;
like($WARN, qr/scalar$/, "Warn method produces warning");

eval { Thread::Subs::shim(\&nap) };
ok($@, "Exception raised on attempt to shim non-thread sub");

$x = Foo->new;
$x->give(111,222,333);
is($x->take->recv, 111, "Threaded object works");

$x = time;
test(2);
ok(eval { Thread::Subs::stop_and_wait(); 1 }, "Stop workers");
cmp_ok(time - $x, '>=', 0.02, "Waited for worker");

done_testing();
