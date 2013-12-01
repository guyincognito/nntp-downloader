use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use IO::Select;
use Net::NNTP;
use XML::Simple;


sub create_children {
    my $child_ct = $_[0];

    my $child_info = {};
    foreach my $i (1 .. $child_ct) {
        pipe (my $pr, my $cw);
        pipe (my $cr, my $pw);
        my $pid = fork();

        if ($pid == 0) {
            close $pr;
            close $pw;
            $cw->autoflush(1);

            while (1) {
                my $msg = receive_message($cr);
                last unless $msg;
                process_message($msg, $cw);
            }
            close $cr;
            close $cw;
            exit 0;
        } else {
            close $cr;
            close $cw;
            $pw->autoflush(1);
            $child_info->{$pid} = {
                pr => $pr, 
                pw => $pw,
            };
        }
    }
    return $child_info;
}

sub get_write_fh {
    my $msg = $_[0];
    my $child_info = $_[1];

    $msg =~ /(\d+)/;
    my $cpid = int $1;
    my $pw = $child_info->{$cpid}{pw};
    return $pw;
}

sub nntp_connect {
    my $param = $_[0];
    my $nntp = Net::NNTP->new("$param->{host}:$param->{port}");
    until (defined $nntp) {
        print "Child $$ sleeping for 60 seconds\n";
        sleep(60);
        $nntp = Net::NNTP->new("$param->{host}:$param->{port}");
    }
    $nntp->authinfo(
            $param->{user}, 
            $param->{pass});
    return $nntp;
}

sub parse_config {
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

sub parse_parameters {
    my $options = {};
    my $result = GetOptions (
        $options,
        'host=s',
        'port=i',
        'output=s',
        'nzb=s',
        'connections=s',
        'help',
    );

    (my $usage = <<'    EOF') =~ s/^[^\S\n]+//gm;
    Usage:
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

    die $usage if not $result or exists $options->{help};

    my $diemsg = '';
    foreach my $opt (qw/host output nzb/) {
        $diemsg .= "Missing $opt\n" unless exists $options->{$opt};
    }
    die $diemsg . $usage if $diemsg ne '';

    # Set default values
    $options->{port} = 119 unless exists $options->{port};
    $options->{connections} = 1 unless exists $options->{connections};

    return $options;
}

sub process_message {
    my $msg = $_[0];
    my $cw = $_[1];

    my $VAR1;
    eval $msg;
    my $nntp = nntp_connect({
            host => $VAR1->{host},
            port => $VAR1->{port},
            user => $VAR1->{user},
            pass => $VAR1->{pass},
    });
    print "Child $$ got part $VAR1->{part}\n";
    my $file = sprintf("%s/%03d", $VAR1->{dir}, $VAR1->{part});

    if (-e $file) {
        print $cw "Child $$ file exists\n";
        return;
    }
    if (ref $VAR1->{data}{groups} eq "ARRAY") {
        print "Child $$ setting group to $VAR1->{data}{groups}[0]\n";
        $nntp->group($VAR1->{data}{groups}[0]);
    } else {
        print "Child $$ setting group to $VAR1->{data}{groups}\n";
        $nntp->group($VAR1->{data}{groups});
    }
    open my $fh, ">", $file;

    foreach my $segment (sort { $a <=> $b } keys $VAR1->{data}{segments}) {
        print "Child $$ downloading segment $segment of $VAR1->{data}{subject}\n";
        my $article = "<" . $VAR1->{data}{segments}{$segment}{content} . ">";
        until (defined $nntp->article($article, $fh)) {
            print "Child $$ unable to download $article, attempting to reconnect\n";
            $nntp->quit();
            $nntp = nntp_connect({
                    host => $VAR1->{host},
                    port => $VAR1->{port},
                    user => $VAR1->{user},
                    pass => $VAR1->{pass}, 
            });
        }
        print $fh "\n";
    }
    print $cw "Child $$ done\n";
    close $fh;
    $nntp->quit();
}

sub receive_message {
    my $fh = $_[0];

    my $bytes = sysread $fh, my $plength, 2;
    return undef if $bytes == 0;
    my $length = unpack "S", $plength;
    print "Child $$ got message length of $length\n";
    my $msg = '';
    do {
        $bytes = sysread $fh, my $read, ($length - length $msg);
        $msg .= $read;
        print "Child $$ read $bytes bytes\n";
    } while length $msg < $length;
    return $msg;
}

sub send_message {
    my $fh = $_[0];
    my $msg = $_[1];
    foreach my $i (qw/length msg/) {
        print $fh $msg->{$i};
    }
}

sub serialize_message {
    my $msg = $_[0];

    $Data::Dumper::Indent = 0;
    my $smsg = Dumper $msg;
    my $plength = pack "S", length $smsg;
    return {
        length => $plength,
        msg => $smsg,
    }
}

my $options = parse_parameters();
my ($username, $password) = parse_config();
mkdir $options->{output} unless -d $options->{output};

my $child_info = create_children($options->{connections});
my $sel = IO::Select->new();
foreach my $pid (keys %$child_info) {
    $sel->add($child_info->{$pid}{pr})
}

my $nzb = XMLin(
        $options->{nzb},
        ForceArray => ['segment'],
        KeyAttr => { segment => 'number' },
        GroupTags => { groups => 'group', segments => 'segment' }
);


my @pid = keys %$child_info;
foreach my $idx (0 .. $#{$nzb->{file}}) {
    my $msg = serialize_message({
            host => $options->{host},
            port => $options->{port},
            user => $username,
            pass => $password,
            dir => $options->{output},
            part => $idx + 1,
            data => $nzb->{file}[$idx],
    });
    print "Writing part " . ($idx + 1);
    print "\n";

    # If idx greater than number of child processes, then we need to wait
    # until one is readable before sending the message
    if ($idx > $#pid) {
        my @ready = $sel->can_read;

        #Use the first available child
        my $pr = $ready[0];
        sysread $pr, my $cmsg, 1024;
        print "Received message $cmsg\n";
        my $pw = get_write_fh($cmsg, $child_info);
        send_message($pw, $msg);
    } else {
        my $pw = $child_info->{$pid[$idx]}{pw};
        send_message($pw, $msg);
    }
}
print "Get remaining child messages\n";
do {
    my @ready = $sel->can_read;
    foreach my $fh (@ready) {
        sysread $fh, my $cmsg, 1024;
        print "Received message $cmsg\n";
        $sel->remove($fh);
    }
} while $sel->count() > 0;
