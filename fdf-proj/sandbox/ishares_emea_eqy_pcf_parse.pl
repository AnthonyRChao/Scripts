#!/bin/env perl

=head2 SCRIPT
  ishares_emea_eqy_pcf_parse.pl

=head2 AUTHOR
  Michael Scharber

=head2 SYNOPSIS
  Parse semi-structured iShares daily EMEA EQY PCF files into well-structured flatfile
  suitable for loading into a database.

=head2 DESCRIPTION
  More to come.

=head2 USAGE

  $0 --inputfile|if|in <filename> [options]
  
  Options :
     --verbose|v               {verbose : be liberal with feedback}

     --debug                   {debug : more information than you want}

     --quiet|q                 {quiet : suppress most feedback}

     --force                   {force : reserved for future use}

     --noheaders|nh            {noheaders : do not output header columns}

     --nocleanup|nc            {nocleanup : leave a trail in working directory for inspection}

     --grain|g                 {grain : dictates the grain of data we are seeking: choices are fund (fund attributes without constituents) and position (fund info + constituent info}. Default is position. 

=cut
use strict;

use vars qw (@FilesToCleanup $NoCleanupOption);
use subs qw (trim);
use warnings;

use Env;
use File::Basename qw(basename);
use File::Copy qw(&copy &move);
use File::Temp qw/ :POSIX /;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use System::Command;
use Time::Piece;

$SIG{INT} = sub {$!=1; die "ABORT: Received SIGINT - cleaning up, stopped";};
$SIG{KILL} = sub {$!=1; die "ABORT: Received SIGKILL - cleaning up, stopped";};
$SIG{HUP} = sub {$!=1; die "ABORT: Received SIGHUP - cleaning up, stopped";};


END {
  #
  # -- Cleanup any temporary files created during our work (if any).
  #
  unlink @FilesToCleanup if ((defined @FilesToCleanup) && (! $NoCleanupOption));
}



#
# -- Configuration
#
our $Usage = "Synopsis: $0 --in <file> [options: See perldoc $0]\n";
our @FilesToCleanup = ();

our $OutputFileParam = undef;   # By default
our $InputFileParam = undef;   # By default
our $NoHeadersOnOutputOption = 0; # By default
our $NoCleanupOption = 0;       # By default
our $LogFileDirectory = "."; # By default
our @Recipients = ("HelpIRESASupport\@blackrock.com");
our $Sender = "HelpIRESASupport\@blackrock.com";


#
# -- Get and vet runtime options.
#
my $forceOption;
my $quietMode = 0;
my $verboseMode = 0;
my $debugMode = 0;
my $grainOption = "position";

my $opts = GetOptions ("inputfile|if|in=s" => \$InputFileParam,
                        "output|of|out=s" => \$OutputFileParam,
                        "nocleanup|nc" => \$NoCleanupOption,
                        "noheaders|nh" => \$NoHeadersOnOutputOption,
                        "quiet|q" => sub { $quietMode=1;$verboseMode=0;$debugMode=0;},
                        "verbose|v" => sub { $quietMode=0;$verboseMode=1;$debugMode=0;},
                        "debug" => sub { $quietMode=0;$verboseMode=1;$debugMode=1; },
                        "force" => \$forceOption,
                        "grain|g=s" => \$grainOption
                      );

our $LocalHostName = `hostname`;
chomp $LocalHostName;

# 0. Prerequisites
#
#
# 0a. Fire up some decent logging
#
Log::Log4perl->easy_init( { level   => $DEBUG, file => "STDERR", layout   => '%d{yyyyMMdd hh:mm:ss} %p %H %P %r: %m%n' });
our $LogObject = get_logger();


# 0b. Test viability of instructions
#
die $Usage
  unless (defined $InputFileParam);


# 1. Open provided input file for consumption purposes
#
unless(-e $InputFileParam) {
    &bailout("Specified input file ($InputFileParam) does not exist...exiting");
}
$LogObject->info("Input file $InputFileParam exists") if ($verboseMode);
eval {
    my $inPipeline = "iconv -f UTF-8 -t ASCII -c $InputFileParam |";
    open(INPUT,$inPipeline) || do {
        &bailout("Could not open provided input file ($InputFileParam) for reading");
    };
    $LogObject->info("Input file $InputFileParam opened with ($inPipeline) for reading") if ($verboseMode);
};
our $SourceCategory = "iShares FTP";
our $SourceName = basename($InputFileParam);


#determine our runtime grain or default if not provided
if (defined $grainOption){
    $LogObject->info("runtime grain is defined as $grainOption") if ($verboseMode);
}

# 2. Start the consumption process
#
my $fundName;
my $fundISIN;
my $fundTicker;
my $fundNavPerShare;
my $fundNavDate;
my $fundEstimatedCashAmount;
my $fundEstimatedCashDate;
my $fundConfirmedCashAmount;
my $fundConfirmedCashDate;
my $fundSizeAmount;
my $fundSizeDate;
my $fundSharesInIssue;
my $fundBaseCurrencyISO;
my $fundPositionDate;
my $fundBidSpread;
my $fundOfferSpread;
my $fundCrReThresh;
my $fundMinBasketSize;
my $fundEstimatedTransFeeAmount;
my $fundConfirmedTransFeeAmount;
my $fundConfirmedTransFeeDate;

my $pcf_indicator_flag = 0;

my @OUTPUT = ();
while (<INPUT> ) {
    # Slurp in the record
    #
    s/[\r\n]+$//;
        unless ( $. % 1000 ) {
        $LogObject->info("Processed $. records from $InputFileParam") if ($verboseMode);
        }
    next unless(/^\w/);
    if ($.==1) {
        if ( /^Fund Name:/ ) {
            $pcf_indicator_flag++;
            $LogObject->info("Located indication of basket type PCF on line $.") if ($debugMode);
        } else {
            &bailout("Expected indication of basket type on line $. - did not find one");
        }
    }
    next unless($pcf_indicator_flag);

    my @_r = split(/\,/,$_);

    if (trim($_r[0]) eq "Fund Name:") {
        $fundName = trim($_r[1]);
        # Look up Fund ISIN using iBP Classification on case-sensitivate name
        #
        my $_fundISINAsString;
        my $_c = "query aeon \"select isin from public.v_etp_mkt_ibp_classification where markit_issue_name='" . $fundName . "' and record_is_current = 'Y' and markit_issue_name not in (select markit_issue_name from public.v_etp_mkt_ibp_classification where record_is_current = 'Y' and markit_family='BlackRock' and markit_issue_name is not null and listing_region <> 'US' group by markit_issue_name having count(*) > 1)\" kettle blk 2>/dev/null | tail -n +2";
        $LogObject->info("Command : ($_c)") if ($verboseMode);
        my $_rs = chase($_c,\$_fundISINAsString);
        
        if (defined $_fundISINAsString){
            chomp($_fundISINAsString);
            $LogObject->info("Command : completed with status ($_rs) ($_fundISINAsString)") if ($verboseMode);
            }
        else {
            $LogObject->info("resort to secondary means to obtain ISIN") if ($verboseMode);
            $fundTicker = trim($_r[2]);
            if (defined $fundTicker){
                $LogObject->info("ticker: $fundTicker") if ($verboseMode);
                my $_c = "query aeon \"select isin from public.v_etp_mkt_ibp_classification where dixie_ticker='" . $fundTicker . "' and record_is_current = 'Y' and markit_family='BlackRock' and dixie_ticker is not null and listing_region <> 'US' and dixie_ticker not in (select dixie_ticker from public.v_etp_mkt_ibp_classification where record_is_current = 'Y' and markit_family='BlackRock' and dixie_ticker is not null and listing_region <> 'US' group by dixie_ticker having count(*) > 1)\" kettle blk 2>/dev/null | tail -n +2";
                $LogObject->info("Command : ($_c)") if ($verboseMode);
                my $_rs = chase($_c,\$_fundISINAsString);
                if (defined $_fundISINAsString){
                    chomp($_fundISINAsString);
                    $LogObject->info("found isin: $_fundISINAsString via ticker: $fundTicker") if (defined $_fundISINAsString);
                }
            }
        }
        
        unless(defined $_fundISINAsString) {
            &bailout("No point in going any further since cannot identify fund by name to get a single ISIN");
        }
        
        $fundISIN = $_fundISINAsString;
        $fundTicker = trim($_r[2]);
        $LogObject->info("Located and extracted information from 'Fund Name' line : $.") if ($debugMode);
        next;
    }
    next unless(defined $fundName);


    if (trim($_r[0]) eq "Total NAV per share:") {
    bailout("Found second instance of 'Total NAV per share' line at $., expecting only one....integrity compromised")
        if (defined $fundNavDate);
    $fundNavPerShare = trim($_r[1]);
    $fundNavDate = trim($_r[2]);
    my $t = Time::Piece->strptime($fundNavDate,"%b %d, %Y") || bailout("Failed to convert Fund Nav Date ($fundNavDate) to a convertible form");
        $fundNavDate = $t->strftime("%Y%m%d"); 
    $LogObject->info("Located and extracted information from 'Total NAV per share' line : $.") if ($debugMode);
        next;
    }

    if (trim($_r[0]) eq "Estimated Cash Component:") {
    bailout("Found second instance of 'Estimated Cash Component' line at $., expecting only one....integrity compromised")
        if (defined $fundEstimatedCashAmount);
    $fundEstimatedCashAmount = trim($_r[1]);
    $fundEstimatedCashDate = trim($_r[2]);
    my $t = Time::Piece->strptime($fundEstimatedCashDate,"%b %d, %Y") || bailout("Failed to convert Fund Cash Date ($fundEstimatedCashDate) to a convertible form");
        $fundEstimatedCashDate = $t->strftime("%Y%m%d"); 
    $LogObject->info("Located and extracted information from 'Estimated Cash Component' line : $.") if ($debugMode);
        next;
    }

    if (trim($_r[0]) eq "Fund Size:") {
    bailout("Found second instance of 'Fund Size' line at $., expecting only one....integrity compromised")
        if (defined $fundSizeAmount);
    $fundSizeAmount = trim($_r[1]);
    $fundSizeDate = trim($_r[2]);
    my $t = Time::Piece->strptime($fundSizeDate,"%b %d, %Y") || bailout("Failed to convert Fund Size Date ($fundSizeDate) to a convertible form");
        $fundSizeDate = $t->strftime("%Y%m%d"); 
    $LogObject->info("Located and extracted information from 'Fund Size' line : $.") if ($debugMode);
        next;
    }

    if (trim($_r[0]) eq "Shares In Issue:") {
        bailout("Found second instance of 'Shares In Issue' line at $., expecting only one....integrity compromised")
        if (defined $fundSharesInIssue);
            $fundSharesInIssue = trim($_r[1]);
            $LogObject->info("Located and extracted information from 'Shares In Issue' line : $.") if ($debugMode);
            next;
        }

    if (trim($_r[0]) eq "Base currency") {
        bailout("Found second instance of 'Base Currency' line at $., expecting only one....integrity compromised")
        if (defined $fundBaseCurrencyISO);
            $fundBaseCurrencyISO = trim($_r[1]);
            $LogObject->info("Located and extracted information from 'Base Currency' line : $.") if ($debugMode);
        next;
    }
    
    #
    # Start of data points specific to the fund level parsing grain
    #   
    if (trim($grainOption) eq "fund") { 
        # actual cash
        if (trim($_r[0]) eq "Confirmed Cash Component:") {
            bailout("Found second instance of 'Confirmed Cash Component' line at $., expecting only one....integrity compromised")
            if (defined $fundConfirmedCashAmount);
                $fundConfirmedCashAmount = trim($_r[1]);
                $fundConfirmedCashDate = trim($_r[2]);
                my $t = Time::Piece->strptime($fundConfirmedCashDate,"%b %d, %Y") || bailout("Failed to convert Fund Confirmed Cash Date ($fundConfirmedCashDate) to a convertible form");
                $fundConfirmedCashDate = $t->strftime("%Y%m%d"); 
                $LogObject->info("Located and extracted information from 'Confirmed Cash Component' line : $.") if ($debugMode);
            next;
        }   
        #basket size
        if (trim($_r[0]) eq "Min Basket Size:") {
            bailout("Found second instance of 'Min Basket Size' line at $., expecting only one....integrity compromised")
            if (defined $fundMinBasketSize);
                $fundMinBasketSize = trim($_r[1]);
                $LogObject->info("Located and extracted information from 'Min Basket Size' line : $.") if ($debugMode);
            next;
        }
        
        # creation fees
        if (trim($_r[0]) eq "Estimated Transaction Fee:") {
            bailout("Found second instance of 'Estimated Transaction Fee' line at $., expecting only one....integrity compromised")
            if (defined $fundEstimatedTransFeeAmount);
                $fundEstimatedTransFeeAmount = trim($_r[1]);
                $LogObject->info("Located and extracted information from 'Estimated Transaction Fee' line : $.") if ($debugMode);
            next;
        }   

        if (trim($_r[0]) eq "Confirmed Transaction Fee:") {
            bailout("Found second instance of 'Confirmed Transaction Fee' line at $., expecting only one....integrity compromised")
            if (defined $fundConfirmedTransFeeAmount);
                $fundConfirmedTransFeeAmount = trim($_r[1]);
                $fundConfirmedTransFeeDate = trim($_r[2]);
                my $t = Time::Piece->strptime($fundConfirmedTransFeeDate,"%b %d, %Y") || bailout("Failed to convert Confirmed Transaction Fee Date ($fundConfirmedTransFeeDate) to a convertible form");
                $fundConfirmedTransFeeDate= $t->strftime("%Y%m%d"); 
                $LogObject->info("Located and extracted information from 'Confirmed Transaction Fee' line : $.") if ($debugMode);
            next;
        }           

        # Start of SAC Rate Data Points
        if (trim($_r[0]) eq "Bid Spread:") {
            bailout("Found second instance of 'Bid Spread' line at $., expecting only one....integrity compromised")
            if (defined $fundBidSpread);
                $fundBidSpread = trim($_r[1]) if (trim($_r[1]) =~ /^\d$/);
                $LogObject->info("Located and extracted information from 'Bid Spread' line : $.") if ($debugMode);
            next;
        }   
        
        if (trim($_r[0]) eq "Offer Spread:") {
            bailout("Found second instance of 'Offer Spread' line at $., expecting only one....integrity compromised")
            if (defined $fundOfferSpread);
                $fundOfferSpread = trim($_r[1]) if (trim($_r[1]) =~ /^\d$/);
                $LogObject->info("Located and extracted information from 'Offer Spread' line : $.") if ($debugMode);
            next;
        }   
        
        if (trim($_r[0]) eq "Threshold for creations/redemptions:") {
            bailout("Found second instance of 'Threshold for creations/redemptions' line at $., expecting only one....integrity compromised")
            if (defined $fundCrReThresh);
                $fundCrReThresh = trim($_r[1]);
                $LogObject->info("Located and extracted information from 'Threshold for creations/redemptions' line : $.") if ($debugMode);
            next;
        }   
        # End of SAC Rate Data Points
    }
    #
    # End of data points specific to the fund level parsing grain
    #  
    
    # parse constituents for position level grain
    #if (trim($grainOption) eq "position") {     
        
        # Read a constituent record of data
        #
        if ((trim($_r[0]) =~ /^\d{8}$/) && (trim($_r[2]) =~ /^\w{7}$/) && (trim($_r[6]) =~ /^\w{12}$/)) {
            my $constituentPositionDate = trim($_r[0]);
            if ((defined $fundPositionDate) && ($constituentPositionDate != $fundPositionDate)) {
                bailout("Found conflicting position dates amongst constituent records at $., expecting a consistent date of ($fundPositionDate)...integrity compromised");
            } 
            else {
                $fundPositionDate = $constituentPositionDate; 
            }
            if (trim($grainOption) eq "position") {
                my $constituentSEDOL = trim($_r[2]);
                my $constituentName = trim($_r[3]);
                my $constituentUnitsInFund = trim($_r[4]);
                my $constituentPrice = trim($_r[5]);
                my $constituentISIN = trim($_r[6]);
                my @_O = ($constituentISIN,$constituentSEDOL,$constituentName,$constituentUnitsInFund,$constituentPrice);
                push(@OUTPUT,\@_O);
            }
            $LogObject->info("Located and extracted a constituent record at line : $.") if ($debugMode);
            next;
        }    

    
}
close INPUT;


# 3. Start the production process
#
#if (scalar @OUTPUT >= 1) {
if (((trim($grainOption) eq "position") && (scalar @OUTPUT >= 1)) || (trim($grainOption) eq "fund")) {
    $LogObject->info("Content (" . (scalar @OUTPUT) . " constituents) parsed successfully....producing output now....") if ($verboseMode);

    # a. Open output file
    #
    #    If not provided --force, and file exists, exit
    #    with success and be done with it.
    #
    unless($forceOption) {
        $LogObject->info("Checking for existence of $OutputFileParam") if ($verboseMode);
        if (-e $OutputFileParam) {
            $LogObject->info("$OutputFileParam exists") if ($verboseMode);
            exit 0;
        }
    }
    if (defined $OutputFileParam) {
        open(OUTPUT,">$OutputFileParam") || do {
        &bailout("Could not open provided output file ($OutputFileParam) for writing");
        };
        select OUTPUT;
        $LogObject->info("$OutputFileParam opened and selected for writing") if ($verboseMode);
    } else {
        select STDOUT;
        $LogObject->info("STDOUT selected for writing") if ($verboseMode);
    }

    
    # output formats are dependent on the grain we are running for
    if (trim($grainOption) eq "position") {
        # b. Write header of the output
        #
        $LogObject->info("formating header for grain of $grainOption") if ($verboseMode);
        print "source_category|source_name|f_position_date|f_isin|f_ticker|f_name|f_nav_per_share|f_nav_date|f_base_currency_iso|f_est_cash_amt|f_est_cash_date|f_shares_in_issue|f_size|f_size_date|c_isin|c_sedol|c_name|c_units_in_basket|c_px\n" unless($NoHeadersOnOutputOption);
        # c. Write the body of the output
        #
        while (@OUTPUT) {
            my $_O = shift @OUTPUT;
            print join("|",$SourceCategory,$SourceName,$fundPositionDate,$fundISIN,$fundTicker,$fundName,$fundNavPerShare,$fundNavDate,$fundBaseCurrencyISO,$fundEstimatedCashAmount,$fundEstimatedCashDate,$fundSharesInIssue,$fundSizeAmount,$fundSizeDate,
            @$_O),"\n";
        }
    }
    if (trim($grainOption) eq "fund") {
        # b. Write header of the output
        #
        $LogObject->info("formating header for grain of $grainOption") if ($verboseMode);
        print "position_date|isin|ticker|name|nav_per_share|nav_date|base_currency_iso|shares_in_issue|aum|aum_date|cu_size|estimated_cash_amt|estimated_cash_date|confirmed_cash_amt|confirmed_cash_date|estimated_trans_fee|confirmed_trans_fee|confirmed_trans_fee_date|bid_spread|ask_spread|create_redeem_threshold|source_category|source_name\n" unless($NoHeadersOnOutputOption);
        # c. Write the body of the output
        #
        print join("|",$fundPositionDate,$fundISIN,$fundTicker,$fundName,$fundNavPerShare,$fundNavDate,$fundBaseCurrencyISO,$fundSharesInIssue,$fundSizeAmount,$fundSizeDate,$fundMinBasketSize,$fundEstimatedCashAmount,$fundEstimatedCashDate,$fundConfirmedCashAmount,$fundConfirmedCashDate,$fundEstimatedTransFeeAmount,$fundConfirmedTransFeeAmount,$fundConfirmedTransFeeDate,$fundBidSpread,$fundOfferSpread,$fundCrReThresh,$SourceCategory,$SourceName,),"\n";
    }
    
    
    # d. Close (if necessary) the output
    #
    if (defined $OutputFileParam) {
        close OUTPUT;
    }    

}


# -- Close up shop and exit
#
$LogObject->info("Normal successful completion of " . basename($0)) if ($verboseMode);

exit 0;


sub bailout {
  my $message = shift @_;
  $LogObject->error($message);
  $!=1;
  die "ABORT";
}

sub trim {
    my $str = shift;
    if (defined $str) {
        $str =~ s/[\^"]//g;
        $str =~ s/^\s+//;
        $str =~ s/\s+$//;
    }

    return $str;
}

sub chase {
    my $_commandAsString = shift;
    my $_refToResponseAsString = undef;
    if ((scalar @_) == 1) {
        $_refToResponseAsString = shift;
    }

    my %_options = ();
    my $_cmd = System::Command->new($_commandAsString,\%_options);
    $LogObject->info("Command has been issued....waiting for command to complete") if ($verboseMode);

    my $_fh_err = $_cmd->stderr();
    my $_fh_out = $_cmd->stdout();

    # Has the child process died yet?
    while (! $_cmd->is_terminated() ) {
        $LogObject->info("Waiting for command to complete") if ($verboseMode);
        eval {
            while (<$_fh_out>) {
                (defined $_refToResponseAsString) ? $$_refToResponseAsString .= $_ : print STDOUT "STDOUT : $_";
            }

            while (<$_fh_err>) {
                (defined $_refToResponseAsString) ? $$_refToResponseAsString .= $_ : print STDERR "STDERR : $_";
            }
        };
        sleep 2;
    }
    $_cmd->close();
    $LogObject->info("Command has completed") if ($verboseMode);

    return $_cmd->exit();
}

