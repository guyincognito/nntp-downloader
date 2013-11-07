use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use Net::NNTP;
use XML::Simple;

my $usage = <<EOF;
--host              name of NNTP host to connect to [REQUIRED]
--port              port of NNTP host to connect to (default 119)
--output            output directory [REQUIRED]
--nzb,              nzb file to process [REQUIRED]
--connections       number of connections to user (default 1)
--help, -?          this message

User name and password is read from the file named nntp.conf in your home
directory. It should have the following format:

username: your user name
password: your_password
EOF
my %options;
my $result = GetOptions (
    \%options,
    'host=s',
    'port=i',
    'output=s',
    'nzb=s',
    'connections=s',
    'help',
);
_parseRequiredParameters(\%options);
my ($user, $pass) = parseConfig();

my $nzb = XMLin(
    $options{nzb},
    ForceArray => ['segment'],
    KeyAttr => { segment => 'number' },
    GroupTags => { groups => 'group', segments => 'segment' }
); 
#print Dumper $nzb;
#exit 0;
#print "connecting to $options{host} on port $options{port}\n";
#my $nntp = nntp_connect(\%options, $user, $pass);

mkdir $options{output} unless -d $options{output};
my $out = 1;

my $child = {};
foreach my $part (@{$nzb->{file}}) {
    if (-e sprintf("%s/%03d", $options{output}, $out)) {
        print "File " . sprintf("%s%03d", $options{output}, $out) . " exists\n";
        ++$out;
        next;
    }
    if (scalar keys %$child >= $options{connections}) {
        check_children($child);
    }
    sleep(10);
    spawn($child, \&child_method, [$out, $part, $user, $pass, \%options]);
    ++$out;
}

#wait for remaining children
while (scalar keys(%$child)) {
    check_children($child);
}

sub nntp_connect {
    my $options = shift;
    my $user = shift;
    my $pass = shift;
    my $nntp = Net::NNTP->new("$options->{host}:$options->{port}", { Debug => 1 });
    while (not defined $nntp) {
        print "Child $$ sleeping for 60 seconds\n";
        sleep(60);
        $nntp = Net::NNTP->new("$options->{host}:$options->{port}");
    }
    $nntp->authinfo($user, $pass);
    return $nntp;
}

sub parseConfig {
    open my $fh, "<", "$ENV{HOME}/nntp.conf";

    my ($username, $password);
    foreach my $line (<$fh>) {
        my ($key, $val) = split ':', $line;
        chomp($key, $val);
        if ($key eq "username") {
            $username = $val;
        } elsif ($key eq "password") {
            $password = $val;
        }
    }
    return ($username, $password);
}

sub _parseRequiredParameters {
    my $options = shift;
    if (!$result || exists $options->{help}) {
        die $usage;
    }
    my $required_missing = 0;
    $required_missing += _testRequiredParam("host", $options);
    $required_missing += _testRequiredParam("output", $options);
    $required_missing += _testRequiredParam("nzb", $options);
    $required_missing += _testRequiredParam("connections", $options);

    die $usage if $required_missing;
}

sub _testRequiredParam {
    my $name = shift;
    my $options = shift;
    if (!exists $options{$name}) {
        print "Missing required option $name\n";
        return 1;
    }
    return 0;
}

sub spawn {
    my $child = $_[0];
    my $method = $_[1];
    my $params = $_[2];
    pipe(my $r, my $w);
    my $pid = fork();
    if ($pid == 0) {
        close $r;
        print "Child $$ starting with part $params->[0]\n";
        $method->($params);
        syswrite $w, ${\"Child $$ done with part $params->[0]\n"};
        close $w;
        exit 0;
    } else {
        close $w;
        print "Adding child $pid for part $params->[0]\n";
        $child->{$pid} = $r;
    }
}

sub check_children {
    my $child = $_[0];
    my $pipes = '';
    while (my ($cpid, $fd) = each %$child) {
        vec($pipes, fileno($fd), 1) = 1;
    }
    my $ready = select($pipes, undef, undef, undef);
    print "Check: We have $ready pipes ready to read\n";
    my @to_delete = ();
    while (my ($cpid, $fd) = each %$child) {
        next unless vec($pipes, fileno($fd), 1) == 1;
        sysread $fd, my $msg, 1024;
        print $msg;
        close $fd;
        push @to_delete, $cpid;
    }
    foreach my $cpid (@to_delete) {
        print "Check: Process $$ Deleting $cpid\n";
        delete $child->{$cpid};
    }
}

sub child_method {
    my $params = $_[0];
    #params needed
    #%options, $user, $pass
    #$out, $part
    my $out = $params->[0];
    my $part = $params->[1];
    my $user = $params->[2];
    my $pass = $params->[3];
    my $options = $params->[4];

    my $nntp = nntp_connect($options, $user, $pass);
    print "Child $$ writing to file $options->{output}" . 
            sprintf("%03d", $out) . "\n";
    open my $fh, ">", $options->{output} . sprintf("%03d", $out);

    if (ref $part->{groups} eq "ARRAY") {
        print "Child $$ setting group to $part->{groups}[0]\n";
        $nntp->group($part->{groups}[0]);
    } else {
        print "Child $$ setting group to $part->{groups}\n";
        $nntp->group($part->{groups});
    }
    foreach my $segment (sort { $a <=> $b } keys $part->{segments}) {
        print "Child $$ downloading segment $segment of $part->{subject}\n";
        my $article = "<" . $part->{segments}{$segment}{content} . ">";
        until (defined $nntp->article($article, $fh)) {
            print "Child $$ unable to download $article, attempting to reconnect\n";
            $nntp->quit();
            $nntp = nntp_connect(\%options, $user, $pass);
        }
        print $fh "\n";
    }
    print "Child $$ finished downloading\n";
    close $fh;
    $nntp->quit();
}
# vim: et sw=4 ts=4
