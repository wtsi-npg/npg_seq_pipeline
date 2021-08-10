use strict;
use warnings;
use Test::More tests => 34;
use Test::Exception;
use Log::Log4perl qw(:levels);
use npg_pipeline::cache::barcodes2;

use_ok(q{npg_pipeline::cache::barcodes2});

#padding for i5 - forward
my $I5_TAG_PAD = q(AC);

#padding for i5 opposite
my $I5_TAG_OPP_PAD = q(GT);

#i7 pad
my $I7_TAG_PAD = q(ATCTC);    # check padding is correct

# mock lims data #
# single sample, no control
my $lims_data1 = {
    1 => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAGGC',
        i7_expected_seq => 'GCATGATA'
    }
};

#single sample , control
my $lims_data2 = {
    888 => { phix_control => 1, i7_expected_seq => 'ACAACGCAATC' },
    1   => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAGGC',
        i7_expected_seq => 'GCATGATA'
    }
};

#multiple sample, no control
# different tags with varying lengths of expected sequence 6,8,12
my $lims_data3 = {
    1 => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAG',
        i7_expected_seq => 'GCATGA'
    },
    2 => {
        phix_control    => 0,
        i5_expected_seq => 'GATTAG',
        i7_expected_seq => 'ACAAGGAC'
    },
    3 => {
        phix_control    => 0,
        i5_expected_seq => 'ACGCCG',
        i7_expected_seq => 'GATGCCGGAGCT'
    },
    4 => {
        phix_control    => 0,
        i5_expected_seq => 'TCCTATCT',
        i7_expected_seq => 'TCCGAC'
    },
    5 => {
        phix_control    => 0,
        i5_expected_seq => 'TTTGCTCG',
        i7_expected_seq => 'CCCATGAC'
    },
    6 => {
        phix_control    => 0,
        i5_expected_seq => 'TACGGTAG',
        i7_expected_seq => 'TCGATCCATGAA'
    },
    7 => {
        phix_control    => 0,
        i5_expected_seq => 'GCCATTCCTTGA',
        i7_expected_seq => 'CTCCCA'
    },
    8 => {
        phix_control    => 0,
        i5_expected_seq => 'TGGGTTTGTGTA',
        i7_expected_seq => 'ATGGTTTA'
    },
    9 => {
        phix_control    => 0,
        i5_expected_seq => 'AGATCGTCCAAG',
        i7_expected_seq => 'CAGGTACAGGTA'
    }
};

# copy of lims_data3 with a control sequence
my $lims_data4 = {
    888 => { phix_control => 1, i7_expected_seq => 'ACAACGCAATC' },
    1   => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAG',
        i7_expected_seq => 'GCATGA'
    },
    2 => {
        phix_control    => 0,
        i5_expected_seq => 'GATTAG',
        i7_expected_seq => 'ACAAGGAC'
    },
    3 => {
        phix_control    => 0,
        i5_expected_seq => 'ACGCCG',
        i7_expected_seq => 'GATGCCGGAGCT'
    },
    4 => {
        phix_control    => 0,
        i5_expected_seq => 'TCCTATCT',
        i7_expected_seq => 'TCCGAC'
    },
    5 => {
        phix_control    => 0,
        i5_expected_seq => 'TTTGCTCG',
        i7_expected_seq => 'CCCATGAC'
    },
    6 => {
        phix_control    => 0,
        i5_expected_seq => 'TACGGTAG',
        i7_expected_seq => 'TCGATCCATGAA'
    },
    7 => {
        phix_control    => 0,
        i5_expected_seq => 'GCCATTCCTTGA',
        i7_expected_seq => 'CTCCCA'
    },
    8 => {
        phix_control    => 0,
        i5_expected_seq => 'TGGGTTTGTGTA',
        i7_expected_seq => 'ATGGTTTA'
    },
    9 => {
        phix_control    => 0,
        i5_expected_seq => 'AGATCGTCCAAG',
        i7_expected_seq => 'CAGGTACAGGTA'
    }
};

#just 8 and 6's to test truncation to length 8
my $lims_data5 = {
    1 => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAG',
        i7_expected_seq => 'GCATGA'
    },
    2 => {
        phix_control    => 0,
        i5_expected_seq => 'GATTAG',
        i7_expected_seq => 'ACAAGGAC'
    },
    3 => {
        phix_control    => 0,
        i5_expected_seq => 'TCCTATCT',
        i7_expected_seq => 'TCCGAC'
    },
    4 => {
        phix_control    => 0,
        i5_expected_seq => 'TTTGCTCG',
        i7_expected_seq => 'CCCATGAC'
    },
};

# just 6 and 8's with control
my $lims_data6 = {
    888 => { phix_control => 1, i7_expected_seq => 'ACAACGCAATC' },
    1   => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAG',
        i7_expected_seq => 'GCATGA'
    },
    2 => {
        phix_control    => 0,
        i5_expected_seq => 'GATTAG',
        i7_expected_seq => 'ACAAGGAC'
    },
    3 => {
        phix_control    => 0,
        i5_expected_seq => 'TCCTATCT',
        i7_expected_seq => 'TCCGAC'
    },
    4 => {
        phix_control    => 0,
        i5_expected_seq => 'TTTGCTCG',
        i7_expected_seq => 'CCCATGAC'
    },
};

# just 6 and 8's with dual-index control
my $lims_data7 = {
    888 => {
        phix_control => 1,
        i7_expected_seq => 'TGTGCAGC',
        i7_expected_seq => 'ACTGATGT'
    },
    1   => {
        phix_control    => 0,
        i5_expected_seq => 'CTCCAG',
        i7_expected_seq => 'GCATGA'
    },
    2 => {
        phix_control    => 0,
        i5_expected_seq => 'GATTAG',
        i7_expected_seq => 'ACAAGGAC'
    },
    3 => {
        phix_control    => 0,
        i5_expected_seq => 'TCCTATCT',
        i7_expected_seq => 'TCCGAC'
    },
    4 => {
        phix_control    => 0,
        i5_expected_seq => 'TTTGCTCG',
        i7_expected_seq => 'CCCATGAC'
    },
};

my $lims_data_cmn_sffx_after_pad = {
    888 => {
        phix_control    => 1,
        i5_expected_seq => 'ACTGATGT',
        i7_expected_seq => 'TGTGCAGC'
    },
    1 => {
        phix_control    => 0,
        i5_expected_seq => 'TTTGGGAC',
        i7_expected_seq => 'CCCGGGAT'
    },
    2 => {
        phix_control    => 0,
        i5_expected_seq => 'TATGGG',
        i7_expected_seq => 'CACGGG'
    },
    3 => {
        phix_control    => 0,
        i5_expected_seq => 'ATAGGG',
        i7_expected_seq => 'ACAGGG'
    }
  };

#testing truncation#
my $lane = npg_pipeline::cache::barcodes2->new(

# got from lims
# dictionary  - key tag index , values -in a dictionary.{ whether control, i5 expected sequence, i7 ex seq}
    lims_data => $lims_data1,

    #get from run
    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
);

my $i5_reads = { 1 => 'CTCCAGGC' };
my $i7_reads = { 1 => 'GCATGATA' };

my $deplexed_i5 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, "",
    $lane->{i5_read_length}, $I5_TAG_PAD );
my $deplexed_i7 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, "",
    $lane->{i7_read_length}, $I7_TAG_PAD );

is_deeply(
    $deplexed_i5,
    { 1 => 'CTCCAG' },
    'Single i5, not control.truncated and padded correctly.'
);
is_deeply(
    $deplexed_i7,
    { 1 => 'GCATGATA' },
'Single i7, not control.truncated and padded correctly. No padding necessary'
);

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data2,

    i7_read_length => 6,
    i5_read_length => 8,
    i5_opposite    => 0,
);

$i7_reads = { 888 => 'ACAACGCA', 1 => 'CTCCAGGC' };
$i5_reads = { 1   => 'GCATGATA' };

$deplexed_i5 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, 888,
    $lane->{i5_read_length}, $I5_TAG_PAD );
$deplexed_i7 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, 888,
    $lane->{i7_read_length}, $I7_TAG_PAD );

is_deeply(
    $deplexed_i7,
    { 1 => 'CTCCAG', 888 => 'ACAACG' },
    'Two i7, one control.truncated and padded correctly.'
);
is_deeply(
    $deplexed_i5,
    { 1 => 'GCATGATA' },
    'Single i5, no control.truncated and padded correctly. No padding necessary'
);

#multiple sample no control, different lengths
$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data3,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
);

$i5_reads = {
    1 => 'CTCCAG',
    2 => 'GATTAG',
    3 => 'ACGCCG',
    4 => 'TCCTATCT',
    5 => 'TTTGCTCG',
    6 => 'TACGGTAG',
    7 => 'GCCATTCCTTGA',
    8 => 'TGGGTTTGTGTA',
    9 => 'AGATCGTCCAAG',
};

$i7_reads = {
    1 => 'GCATGA',
    2 => 'ACAAGGAC',
    3 => 'GATGCCGGAGCT',
    4 => 'TCCGAC',
    5 => 'CCCATGAC',
    6 => 'TCGATCCATGAA',
    7 => 'CTCCCA',
    8 => 'ATGGTTTA',
    9 => 'CAGGTACAGGTA',
};

$deplexed_i5 = npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, '',
    $lane->{i5_read_length}, $I5_TAG_PAD );
$deplexed_i7 = npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, '',
    $lane->{i7_read_length}, $I7_TAG_PAD );

is_deeply(
    $deplexed_i5,
    {
        1 => 'CTCCAG',
        2 => 'GATTAG',
        3 => 'ACGCCG',
        4 => 'TCCTAT',
        5 => 'TTTGCT',
        6 => 'TACGGT',
        7 => 'GCCATT',
        8 => 'TGGGTT',
        9 => 'AGATCG',
    },
'multiple i5, no control.truncated and padded correctly (no padding necessary).'
);
is_deeply(
    $deplexed_i7,
    {
        1 => 'GCATGAAT',
        2 => 'ACAAGGAC',
        3 => 'GATGCCGG',
        4 => 'TCCGACAT',
        5 => 'CCCATGAC',
        6 => 'TCGATCCA',
        7 => 'CTCCCAAT',
        8 => 'ATGGTTTA',
        9 => 'CAGGTACA'
    },
    'Multiple i7, no control.truncated and padded correctly.'
);

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data4,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
);

$i5_reads = {
    1   => 'CTCCAG',
    2   => 'GATTAG',
    3   => 'ACGCCG',
    4   => 'TCCTATCT',
    5   => 'TTTGCTCG',
    6   => 'TACGGTAG',
    7   => 'GCCATTCCTTGA',
    8   => 'TGGGTTTGTGTA',
    9   => 'AGATCGTCCAAG'
};
$i7_reads = {
    1 => 'GCATGA',
    2 => 'ACAAGGAC',
    3 => 'GATGCCGGAGCT',
    4 => 'TCCGAC',
    5 => 'CCCATGAC',
    6 => 'TCGATCCATGAA',
    7 => 'CTCCCA',
    8 => 'ATGGTTTA',
    9 => 'CAGGTACAGGTA',
    888 => 'ACAACGCAATC'
};

$deplexed_i5 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, 888,
    $lane->{i5_read_length}, $I5_TAG_PAD );
$deplexed_i7 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, 888,
    $lane->{i7_read_length}, $I7_TAG_PAD );

is_deeply(
    $deplexed_i5,
    {
        1   => 'CTCCAG',
        2   => 'GATTAG',
        3   => 'ACGCCG',
        4   => 'TCCTAT',
        5   => 'TTTGCT',
        6   => 'TACGGT',
        7   => 'GCCATT',
        8   => 'TGGGTT',
        9   => 'AGATCG',
    },
'Multiple i5, with control. Truncated and padded correctly (no padding necessary).'
);
is_deeply(
    $deplexed_i7,
    {
        1 => 'GCATGAAT',
        2 => 'ACAAGGAC',
        3 => 'GATGCCGG',
        4 => 'TCCGACAT',
        5 => 'CCCATGAC',
        6 => 'TCGATCCA',
        7 => 'CTCCCAAT',
        8 => 'ATGGTTTA',
        9 => 'CAGGTACA',
        888 => 'ACAACGCA'
    },
    'Multiple i7, with control. Truncated and padded correctly'
);

#lims_data4 with i5 opposite
$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data4,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 1,
);

$i5_reads = {
    1   => 'CTCCAG',
    2   => 'GATTAG',
    3   => 'ACGCCG',
    4   => 'TCCTATCT',
    5   => 'TTTGCTCG',
    6   => 'TACGGTAG',
    7   => 'GCCATTCCTTGA',
    8   => 'TGGGTTTGTGTA',
    9   => 'AGATCGTCCAAG'
};
$i7_reads = {
    1 => 'GCATGA',
    2 => 'ACAAGGAC',
    3 => 'GATGCCGGAGCT',
    4 => 'TCCGAC',
    5 => 'CCCATGAC',
    6 => 'TCGATCCATGAA',
    7 => 'CTCCCA',
    8 => 'ATGGTTTA',
    9 => 'CAGGTACAGGTA',
    888 => 'ACAACGCAATC'
};

$deplexed_i5 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, 888,
    $lane->{i5_read_length},
    $I5_TAG_OPP_PAD );

is_deeply(
    $deplexed_i5,
    {
        1   => 'CTCCAG',
        2   => 'GATTAG',
        3   => 'ACGCCG',
        4   => 'TCCTAT',
        5   => 'TTTGCT',
        6   => 'TACGGT',
        7   => 'GCCATT',
        8   => 'TGGGTT',
        9   => 'AGATCG'
    },
'Multiple i5, no control. Truncated and padded correctly (no padding necessary). i5 opposite.'
);
$deplexed_i7 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, 888,
    $lane->{i7_read_length}, $I7_TAG_PAD );

is_deeply(
    $deplexed_i7,
    {
        '1' => 'GCATGAAT',
        '2' => 'ACAAGGAC',
        '3' => 'GATGCCGG',
        '4' => 'TCCGACAT',
        '5' => 'CCCATGAC',
        '6' => 'TCGATCCA',
        '7' => 'CTCCCAAT',
        '8' => 'ATGGTTTA',
        '8' => 'ATGGTTTA',
        '9' => 'CAGGTACA',
        '888' => 'ACAACGCA'
    },
    'Multiple i7. with control. Truncated and padded correctly.'
);

# checking when read length is 8 for i5 and 6 for i7 with i5 opposite
$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data5,

    i5_read_length => 8,
    i7_read_length => 6,
    i5_opposite    => 0,
);

$i5_reads = { 1 => 'CTCCAG', 2 => 'GATTAG', 3 => 'TCCTATCT', 4 => 'TTTGCTCG' };
$i7_reads = { 1 => 'GCATGA', 2 => 'ACAAGGAC', 3 => 'TCCGAC', 4 => 'CCCATGAC' };

$deplexed_i5 = npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, '',
    $lane->{i5_read_length}, $I5_TAG_PAD );
$deplexed_i7 = npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, '',
    $lane->{i7_read_length}, $I7_TAG_PAD );

is_deeply(
    $deplexed_i5,
    { 1 => 'CTCCAGAC', 2 => 'GATTAGAC', 3 => 'TCCTATCT', 4 => 'TTTGCTCG' },
    'Multiple i5. no control. Truncated and padded correctly.'
);
is_deeply(
    $deplexed_i7,
    { 1 => 'GCATGA', 2 => 'ACAAGG', 3 => 'TCCGAC', 4 => 'CCCATG' },
'Multiple i7. no control. Truncated and padded correctly. (no padding needed).'
);

#i5 opposite with lims_data5
$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data5,

    i5_read_length => 10,
    i7_read_length => 6,
    i5_opposite    => 1,
);

$i5_reads = { 1 => 'CTCCAG', 2 => 'GATTAG', 3 => 'TCCTATCT', 4 => 'TTTGCTCGGT' };
$i7_reads = { 1 => 'GCATGA', 2 => 'ACAAGGAC', 3 => 'TCCGAC', 4 => 'CCCATGACAT' };

throws_ok {
    npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, '',
        $lane->{i5_read_length},
        $I5_TAG_OPP_PAD )
}
qr/Cannot extend for more bases than in padding sequence/,
  'Throws when padding needed is longer than length of padding sequence';

#i5 opposite with lims data 6
$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data6,

    i5_read_length => 10,
    i7_read_length => 6,
    i5_opposite    => 1,
);

$i5_reads = {
    1   => 'CTCCAG',
    2   => 'GATTAG',
    3   => 'TCCTATCT',
    4   => 'TTTGCTCG'
};
$i7_reads = {
    888 => 'ACAACGCAATC',
    1   => 'GCATGA',
    2   => 'ACAAGGAC',
    3   => 'TCCGAC',
    4   => 'CCCATGAC' };

$deplexed_i5 = npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, 888,
    $lane->{i5_read_length}, $I5_TAG_OPP_PAD );
is_deeply(
    $deplexed_i5,
    { 888 => 'ACAACGCA', 1 => 'GCATGAGT', 2 => 'ACAAGGAC', 3 => 'TCCGACGT', 4 => 'CCCATGAC' },
'Multiple i5. Truncated and padded correctly'
);

$deplexed_i7 = npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, 888,
    $lane->{i7_read_length}, $I7_TAG_PAD );
is_deeply(
    $deplexed_i7,
    { 888 => 'ACAACG', 1 => 'GCATGA', 2 => 'ACAAGG', 3 => 'TCCGAC', 4 => 'CCCATG' },
'Multiple i7. Truncated and padded correctly. (no padding needed).'
);

#lims_data6
$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data6,

    i5_read_length => 8,
    i7_read_length => 6,
    i5_opposite    => 0,
);

$i5_reads = {
    1   => 'CTCCAG',
    2   => 'GATTAG',
    3   => 'TCCTATCT',
    4   => 'TTTGCTCG'
};
$i7_reads = { 888 => 'ACAACGCAATC', 1 => 'GCATGA', 2 => 'ACAAGGAC', 3 => 'TCCGAC', 4 => 'CCCATGAC' };

$deplexed_i5 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i5_reads, 888,
    $lane->{i5_read_length}, $I5_TAG_PAD );

is_deeply(
    $deplexed_i5,
    {
        1   => 'CTCCAGAC',
        2   => 'GATTAGAC',
        3   => 'TCCTATCT',
        4   => 'TTTGCTCG'
    },
'Multiple i5. No control. Truncated and padded correctly. i5_read_length 8.'
);

$deplexed_i7 =
  npg_pipeline::cache::barcodes2::_truncate_and_pad( $i7_reads, 888,
    $lane->{i7_read_length}, $I7_TAG_PAD );
is_deeply(
    $deplexed_i7,
    { 888 => 'ACAACG', 1 => 'GCATGA', 2 => 'ACAAGG', 3 => 'TCCGAC', 4 => 'CCCATG' },
'Multiple i7. no control. Truncated and padded correctly (no padding neccesary)'
);

#Tests for removing common suffix
my $common_suffix_1c = {
    888 => 'ACGCAA',
    1   => 'ACGTAA'
};

$deplexed_i7 =
    npg_pipeline::cache::barcodes2::_remove_common_suffixes( $common_suffix_1c,
        888 );
is_deeply($deplexed_i7, $common_suffix_1c,
'One control and one non-control seq no common suffix removed');

my $common_suffix_1 = { 1 => 'ACGTAA' };

$deplexed_i7 =
    npg_pipeline::cache::barcodes2::_remove_common_suffixes( $common_suffix_1);
is_deeply($deplexed_i7, $common_suffix_1,
'No control and one non-control seq no common suffix removed');

my $common_suffix_control = {
    888 => 'ACGTTCAG',
    1   => 'AAATCCAG',
    2   => 'AAACCCAG',
    3   => 'ACAGCCAG',
    4   => 'CACACCAG',
};

my $removed_suffix_seq_with_control =
  npg_pipeline::cache::barcodes2::_remove_common_suffixes(
    $common_suffix_control, 888 );
is_deeply(
    $removed_suffix_seq_with_control,
    { 888 => 'ACGT', 1 => 'AAAT', 2 => 'AAAC', 3 => 'ACAG', 4 => 'CACA' },
    'common suffix of CCAG is removed from all (included phix control)'
);

# all ending in CCAG, no control
my $common_suffix_no_control = {
    1 => 'AAATCCAG',
    2 => 'AAACCCAG',
    3 => 'ACAGCCAG',
    4 => 'CACACCAG',
};

my $removed_suffix_seq_no_control =
  npg_pipeline::cache::barcodes2::_remove_common_suffixes(
    $common_suffix_no_control, "" );
is_deeply(
    $removed_suffix_seq_no_control,
    { 1 => 'AAAT', 2 => 'AAAC', 3 => 'ACAG', 4 => 'CACA' },
    'Common suffix of CCAG is removed from all. No control.'
);

my $common_suffix_all_same = {
    1 => 'AAACCCAG',
    2 => 'AAACCCAG',
    3 => 'AAACCCAG',
    4 => 'AAACCCAG',
};
my $removed_suffix_seq_all_same =
  npg_pipeline::cache::barcodes2::_remove_common_suffixes(
    $common_suffix_all_same, "" );
is_deeply(
    $removed_suffix_seq_all_same,
    { 1 => '', 2 => '', 3 => '', 4 => '' },
    'Common suffix of AAACCCAG is removed from all. No control.'
);

#testing whole run
lives_ok {
  $lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data1,

    i5_read_length => 8,
    i7_read_length => 6,
    i5_opposite    => 0,
  );
} q{only one sample no control ok};

lives_ok {
  $lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data2,

    i5_read_length => 8,
    i7_read_length => 6,
    i5_opposite    => 0,
  );
} q{only one sample control ok};

lives_ok {
  $lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data3,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
  );
} q{multiple samples no control ok};

my $expected_deplexed_reads = {
    '1' => {
        'phix_control' => 0,
        'i5_read'      => 'CTCCAG',
        'i7_read'      => 'GCATGAAT'
    },
    '2' => {
        'i7_read'      => 'ACAAGGAC',
        'i5_read'      => 'GATTAG',
        'phix_control' => 0
    },
    '3' => {
        'phix_control' => 0,
        'i7_read'      => 'GATGCCGG',
        'i5_read'      => 'ACGCCG'
    },
    '4' => {
        'phix_control' => 0,
        'i5_read'      => 'TCCTAT',
        'i7_read'      => 'TCCGACAT'
    },
    '5' => {
        'phix_control' => 0,
        'i5_read'      => 'TTTGCT',
        'i7_read'      => 'CCCATGAC'
    },
    '6' => {
        'phix_control' => 0,
        'i7_read'      => 'TCGATCCA',
        'i5_read'      => 'TACGGT'
    },
    '7' => {
        'phix_control' => 0,
        'i5_read'      => 'GCCATT',
        'i7_read'      => 'CTCCCAAT'
    },
    '8' => {
        'i7_read'      => 'ATGGTTTA',
        'i5_read'      => 'TGGGTT',
        'phix_control' => 0
    },
    '9' => {
        'phix_control' => 0,
        'i5_read'      => 'AGATCG',
        'i7_read'      => 'CAGGTACA'
    },
};

my $deplexed_reads = $lane->run();
is_deeply( $deplexed_reads, $expected_deplexed_reads,
    'whole run without phix control, padding of i7.' );

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data4,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
);

$expected_deplexed_reads = {
    '1' => {
        'phix_control' => 0,
        'i5_read'      => 'CTCCAG',
        'i7_read'      => 'GCATGAAT',
    },
    '2' => {
        'phix_control' => 0,
        'i5_read'      => 'GATTAG',
        'i7_read'      => 'ACAAGGAC',
    },
    '3' => {
        'phix_control' => 0,
        'i5_read'      => 'ACGCCG',
        'i7_read'      => 'GATGCCGG',
    },
    '4' => {
        'phix_control' => 0,
        'i5_read'      => 'TCCTAT',
        'i7_read'      => 'TCCGACAT',
    },
    '5' => {
        'phix_control' => 0,
        'i5_read'      => 'TTTGCT',
        'i7_read'      => 'CCCATGAC',
    },
    '6' => {
        'phix_control' => 0,
        'i5_read'      => 'TACGGT',
        'i7_read'      => 'TCGATCCA',
    },
    '7' => {
        'phix_control' => 0,
        'i5_read'      => 'GCCATT',
        'i7_read'      => 'CTCCCAAT',
    },
    '8' => {
        'phix_control' => 0,
        'i5_read'      => 'TGGGTT',
        'i7_read'      => 'ATGGTTTA',
    },
    '9' => {
        'phix_control' => 0,
        'i5_read'      => 'AGATCG',
        'i7_read'      => 'CAGGTACA',
    },
    '888' => {
        'phix_control' => 1,
        'i5_read'      => 'TCTTTC',
        'i7_read'      => 'ACAACGCA',
    }
};

$deplexed_reads = $lane->run();
is_deeply( $deplexed_reads, $expected_deplexed_reads,
    'whole run on lims data with phix control' );

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data4,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 1,
);

$expected_deplexed_reads = {
    '1' => {
        'phix_control' => 0,
        'i7_read'      => 'GCATGAAT',
        'i5_read'      => 'CTCCAG',
    },
    '2' => {
        'phix_control' => 0,
        'i5_read'      => 'GATTAG',
        'i7_read'      => 'ACAAGGAC',
    },
    '3' => {
        'phix_control' => 0,
        'i5_read'      => 'ACGCCG',
        'i7_read'      => 'GATGCCGG',
    },
    '4' => {
        'phix_control' => 0,
        'i7_read'      => 'TCCGACAT',
        'i5_read'      => 'TCCTAT',
    },
    '5' => {
        'phix_control' => 0,
        'i5_read'      => 'TTTGCT',
        'i7_read'      => 'CCCATGAC'
    },
    '6' => {
        'phix_control' => 0,
        'i5_read'      => 'TACGGT',
        'i7_read'      => 'TCGATCCA',
    },
    '7' => {
        'phix_control' => 0,
        'i5_read'      => 'GCCATT',
        'i7_read'      => 'CTCCCAAT',
    },
    '8' => {
        'phix_control' => 0,
        'i5_read'      => 'TGGGTT',
        'i7_read'      => 'ATGGTTTA',
    },
    '9' => {
        'phix_control' => 0,
        'i5_read'      => 'AGATCG',
        'i7_read'      => 'CAGGTACA',
    },
    '888' => {
        'phix_control' => 1,
        'i5_read'      => 'AGATCT',
        'i7_read'      => 'ACAACGCA',
    }
};

$deplexed_reads = $lane->run();
is_deeply( $deplexed_reads, $expected_deplexed_reads,
'whole run passes on lims data with phix control when i5 opposite, (and when padding i5 not necessary)'
);

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data5,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
);

$expected_deplexed_reads = {
    '1' => {
        'i5_read'      => 'CTCCAG',
        'i7_read'      => 'GCATGAAT',
        'phix_control' => 0
    },
    '2' => {
        'i5_read'      => 'GATTAG',
        'phix_control' => 0,
        'i7_read'      => 'ACAAGGAC'
    },
    '3' => {
        'phix_control' => 0,
        'i7_read'      => 'TCCGACAT',
        'i5_read'      => 'TCCTAT'
    },
    '4' => {
        'i5_read'      => 'TTTGCT',
        'phix_control' => 0,
        'i7_read'      => 'CCCATGAC'
    }
};

$deplexed_reads = $lane->run();
is_deeply( $deplexed_reads, $expected_deplexed_reads,
    'whole run on lims data with i7 padding' );

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data5,

    i5_read_length => 8,
    i7_read_length => 6,
    i5_opposite    => 0,
);

$expected_deplexed_reads = {
    '1' => {
        'i7_read'      => 'GCATGA',
        'i5_read'      => 'CTCCAGAC',
        'phix_control' => 0
    },
    '2' => {
        'phix_control' => 0,
        'i5_read'      => 'GATTAGAC',
        'i7_read'      => 'ACAAGG'
    },
    '3' => {
        'phix_control' => 0,
        'i5_read'      => 'TCCTATCT',
        'i7_read'      => 'TCCGAC'
    },
    '4' => {
        'phix_control' => 0,
        'i5_read'      => 'TTTGCTCG',
        'i7_read'      => 'CCCATG'
    },
};

$deplexed_reads = $lane->run();
is_deeply( $deplexed_reads, $expected_deplexed_reads,
    'whole run on lims data with i5 padding' );

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data4,

    i5_read_length => 10,
    i7_read_length => 6,
    i5_opposite    => 1,
);
throws_ok { $lane->run() }
qr/Cannot extend for more bases than in padding sequence/,
  'croaks whole run on lims data with i5 opposite (when i5 needs padding)';

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data6,

    i5_read_length => 6,
    i7_read_length => 8,
    i5_opposite    => 0,
);

$expected_deplexed_reads = {
    '1' => {
        'i7_read'      => 'GCATGAAT',
        'i5_read'      => 'CTCCAG',
        'phix_control' => 0
    },
    '2' => {
        'i7_read'      => 'ACAAGGAC',
        'i5_read'      => 'GATTAG',
        'phix_control' => 0
    },
    '3' => {
        'i7_read'      => 'TCCGACAT',
        'i5_read'      => 'TCCTAT',
        'phix_control' => 0
    },
    '4' => {
        'phix_control' => 0,
        'i7_read'      => 'CCCATGAC',
        'i5_read'      => 'TTTGCT'
    },
    '888' => {
        'i7_read'      => 'ACAACGCA',
        'i5_read'      => 'TCTTTC',
        'phix_control' => 1
    },
};

$deplexed_reads = $lane->run();
is_deeply( $deplexed_reads, $expected_deplexed_reads,
    'whole run on lims data with i7 padding' );

$lane = npg_pipeline::cache::barcodes2->new(
    lims_data => $lims_data_cmn_sffx_after_pad,

    i5_read_length => 8,
    i7_read_length => 8,
    i5_opposite    => 0,
);

$expected_deplexed_reads = {
    '1' => {
        'i5_read'      => 'TTT',
        'i7_read'      => 'CCC',
        'phix_control' => 0
    },
    '2' => {
        'i5_read'      => 'TAT',
        'i7_read'      => 'CAC',
        'phix_control' => 0
    },
    '3' => {
        'i5_read'      => 'ATA',
        'i7_read'      => 'ACA',
        'phix_control' => 0
    },
    '888' => {
        'i5_read'      => 'ACT',
        'i7_read'      => 'TGT',
        'phix_control' => 1
    },
};

$deplexed_reads = $lane->run();

is_deeply( $deplexed_reads, $expected_deplexed_reads,
    'Whole run where padded sequences have their suffixes removed' );
1;
