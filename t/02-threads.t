#!perl
use 5.010;
use strict;
use warnings;
BEGIN { eval("use threads") or $::ERR = $@ }
use threads::shared;
my $WARN :shared = '';
BEGIN { close STDERR; open STDERR, '>', \$WARN or die "$!" }
use Test::More;
BEGIN {
    unless (threads->can('create')) {
        diag("threads failed: $::ERR") if $::ERR;
        BAIL_OUT("All further testing requires threads");
    }
}
use Thread::Subs (
    attributes => 1,
    autostart  => 10,
    );
use Time::HiRes qw(time);

sub nap { select(undef, undef, undef, $_[0] * 0.01); return @_ }

sub test  :Thread         { &nap }
sub dies  :Thread         { nap(5); die "@_\n" }
sub clim1 :Thread(clim=1) { &nap }
sub qlim1 :Thread(qlim=1) { &nap }
sub void  :Thread(void)   { die "@_\n" if @_ }

sub busy_workers {
    my %t = &Thread::Subs::current_tasks;
    return scalar grep { $_ } values(%t)
}

sub all_idle_ok {
    for (1..5) { last unless &busy_workers; nap(1) }
    cmp_ok(&busy_workers, '==', 0, "All workers idle");
}

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
clim1($_)->cb(sub { $x .= '1' })
    for (3, 2); # sequential
test($_)->cb(sub { $x .= '0' })
    for (1, 2, 4); # parallel
test(6)->recv; # delay
is($x, '00101', "Expected order of completion");

&all_idle_ok;

$x = time;
test(2) for 0..9; # all workers busy next 20ms
qlim1(2); # should block
$x = time - $x;
cmp_ok($x, '>=', 0.02, "Blocked by queue limit");

is($WARN, '', "No warnings");
dies("void");
$x = dies("scalar");
dies("ignored")->data; # forces wait
&all_idle_ok;
like($WARN, qr/void$/, "Void context exception produces warning");
eval { $x->fatal };
like($@, qr/scalar$/, "Fatal method produces exception");

$x = time;
void("void2");
test(2);
ok(eval { Thread::Subs::stop_and_wait(); 1 }, "Stop workers");
cmp_ok(time - $x, '>=', 0.02, "Waited for worker");
print "## $WARN";
like($WARN, qr/void2$/, "Void sub exception produced warning");

done_testing();
