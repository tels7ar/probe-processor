#!/usr/bin/perl -w

# 1)  Average Response time of the top 10 most frequent probe targets over the 3 hour period
# 2)  Count of number of probes sent per minute over the 3 hour period
# 3)  The maximum probe time for each probe source over the 3 hour period

# each second of data looks like this:

#"2015-02-06T06:36:36+00:00": [
#    {
#       "source": "source13",
#       "target": "target176",
#       "sample_ms": 875
#    },
# (there are multiple entries (probes) per second)

use JSON::Parse 'json_file_to_perl';
use Data::Dumper;

# I use a lot of hashes because it's an easy datastructure to work with
# in this case.  However, it's probably not the most efficient solution
# since I end up having to sort the hashes.  Still, it works ok here.
my %TargetCount = ();
my $TargetSum = 0;
my %SourceMax = ();
my %ProbesPerMinute = ();

my @MostFreqTargets = ();

($#ARGV == 0) or die "usage: $0 <data file>";

# Slurp the input file into a perl data structure and extract
# some info from it for later processing.
sub ProcessInput
{
    # this won't scale well with very large files since
    # we are reading it all in to memory but what the heck
    my $input = json_file_to_perl ($ARGV[0]);

    # The keys of %input are the timestamps
    # Each timestamp value is an array entry
    # Each array contains a hash
    # Each hash is a probe containing keys of source, target, sample_ms
    while ( my ($timestamp, $probes ) = each (%{$input}) )
    {
        # Create a hash key from the timestamp for each minute.
        my ($MinuteKey) = ($timestamp =~ /(\d+-\d+-\d+T\d+:\d+):/);
        my $ProbeCount = 0;
        foreach my $probe (@$probes)
        {
            $ProbeCount++;
            # this is just a lot easier to read
            my $target = ${$probe}{'target'};
            my $source = ${$probe}{'source'};
            my $sample_ms = ${$probe}{'sample_ms'};

            # Count how many times we've seen this probe
            $TargetCount{$target}++;
            # Accumulate the time for each probe
            $TargetSum{$target} += ${$probe}{'sample_ms'};

            # just set the SourceMax if we've never seen it before
            if ( ! exists $SourceMax{$source} )
            {
                $SourceMax{$source} = $sample_ms;
            } elsif ( $sample_ms > $SourceMax{$source} )
            {
                # otherwise, if this sample has a higher value than
                # previously seen, save it in $SourceMax
                $SourceMax{$source} = $sample_ms;
            }
        }
        $ProbesPerMinute{$MinuteKey} = $ProbeCount;
    }
}


sub CountTargets
{
    # %TargetCount now contains a count of how many times each target appears.
    # Find the keys with the 10 highest values.
    # Again, not the most efficient way to go about this, but sufficient
    # for this dataset.
    my @keys = sort {
        $TargetCount{$b} <=> $TargetCount{$a}
    } keys %TargetCount;
    # and take the first 10.
    @MostFreqTargets = @keys[0..9];
}

sub PrintAvgResponses
{
    print "Average response time for top 10 most frequent targets (in ms):\n";
    foreach my $target (@MostFreqTargets)
    {
        # Now that we have sums and counts for each target it is a simple
        # matter to do some division to get the average.  For larger
        # datasets this could be changed to a moving average to avoid
        # having to keep a counter and accumulator (which could overflow).
        my $avg = $TargetSum{$target} / $TargetCount{$target};
        print "$target: $avg\n";
    }
}

sub PrintProbesPerMinute
{
    # Print the number of probes for each minute
    # TODO: This should probably be improved to print '0' for
    # minutes in which no probes occur.
    print "\nNumber of probes per minute:\n";
    # Do a string compare to sort this out.
    foreach my $Minute (sort {$a cmp $b} keys %ProbesPerMinute)
    {
        print "$Minute: $ProbesPerMinute{$Minute}\n";
    }
}

sub PrintMaxTimes
{
    # Print the max time for each source
    print "\nMaximum time for each source (in ms):\n";
    # Do an alpha sort to make the output look nice. The format of the
    # datestrings ensures that this works even through day rollover, etc.
    foreach my $source (sort {$a cmp $b} keys %SourceMax)
    {
        print $source . ": " . $SourceMax{$source} . "\n";
    }
}

ProcessInput;
CountTargets;
PrintAvgResponses;
PrintProbesPerMinute;
PrintMaxTimes;

# all done
exit 0;
