package npg_pipeline::validation::s3;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Perl6::Slurp;
use English qw/ -no_match_vars /;
use List::MoreUtils qw/ any /;
use File::Spec::Functions qw/ catfile /;

with qw/ npg_pipeline::product::cache_merge
         npg_pipeline::validation::common /;

our $VERSION = '0';

Readonly::Scalar my $RECIEPT_FILE_NAME_PATTERN =>
                 qr{/returned_20[[:digit:]]{2} # year
                     (?:-[[:digit:]]{2})       # -month
                     (?:-[[:digit:]]{2})       # -date
                     [.]txt[.]bz2\Z}smx;
Readonly::Scalar my $RECIEPT_ROW_DELIM         => qq[\t];
Readonly::Scalar my $RECIEPT_NUM_COLUMNS       => 4;
Readonly::Scalar my $RECIEPT_RECEIVED_FLAG     => q[correct];

=head1 NAME

npg_pipeline::validation::s3

=head1 SYNOPSIS

  my $v = npg_pipeline::validation::s3->new(
    product_entities => $product_entities,
  );
  my $deletable = $v->fully_archived;
  my @archived_entities = $v->eligible_product_entities();

=head1 DESCRIPTION

Validation of files present in a pre-configures s3 location
against files present on staging. Only files that belong
to products that should have been arcived to s3 are considered.
Customer's receipts are used to confirm that data product
files have been received by a customer. If no receipt is
available for a data product, we expect that either the
product has failed QC or it is a candidate for topping up.
If neither of the latter assumtions is true, the product
is not consided successfully archived.

The validation is implemented as a 'lazy' evaluation of
a number of conditions, i.e. if the earlier condition
confirms the archival, the conditions following after it
are not evaluated. The order of evaluation is as flollows:
received by the customer, failed QC outcome, available in
a merged component cache.

Data product sent to the customer always have 'Accepted final'
library QC outcome. We check that all products received by the
customer still have this outcome. If not, we disregard the
fact that the product had been received by the customer and
progress to the next step of validation.

=head1 SUBROUTINES/METHODS

=head2 qc_schema

Attribute, required, DBIx schema object for the QC database.
Inherited from npg_pipeline::product::release. The builder
method for this attribute in this class returns an undefined
value to force passing the database connection object from
the caller.

=cut

has '+qc_schema' => (
  builder    => '_build_undef_qc_schema',
);
sub _build_undef_qc_schema {
  return;
}

=head2 file_extension

Attribute, inherited from npg_pipeline::validation::common

=head2 product_entities

Inherited from npg_pipeline::validation::common

=head2 eligible_product_entities

Inherited from npg_pipeline::validation::common

=head2 build_eligible_product_entities

Builder method for the eligible_product_entities attribute.

=cut

sub build_eligible_product_entities {
  my $self = shift;
  @{$self->product_entities}
    or $self->logcroak('product_entities array cannot be empty');
  my @p =
    grep { $self->is_release_data($_->target_product) &&
           $self->is_for_s3_release($_->target_product) }
    @{$self->product_entities};
  return \@p;
}

=head2 fully_archived

Returns true if each of the products eligible for s3 archival is
either archived or satisfies a condition that makes archival of this
product in its current state unnecessary.

=cut

sub fully_archived {
  my $self = shift;

  my $archived = 1;
  foreach my $p (map { $_->target_product }
                 @{$self->eligible_product_entities}) {
    ($self->_received_by_customer($p) and $self->_passed_mqc($p))
    or $self->_failed_mqc($p)
    or $self->_saved4topup($p)
    or ($archived = 0);
  }

  return $archived;
}

has '_cached_receipts' => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__cached_receipts {
  my $self = shift;

  my $cache = {};
  my $dirs  = {};
  foreach my $e (@{$self->eligible_product_entities}) {

    my $p = $e->target_product;
    my $dir = $self->receipts_location($p);
    (defined $dir) or $self->logcroak(
      'Failed to retrieve receipts location for ' .
      $p->composition->freeze());
    next if $dirs->{$dir}; # Already dealt with this directory

    #####
    # Look at all relevant to this product's study receipts.
    #
    $self->debug("Looking at receipts in $dir");
    $dirs->{$dir} = 1;
    my @sample_receipts = ();
    opendir my $dh, $dir or $self->logcroak(
      "Failed to open receipts directory $dir for reading: $OS_ERROR");
    while (readdir $dh) { # this way of iteration requires Perl 5.012
      my $path = join q[/], $dir, $_;
      next if (not -f $path);
      push @sample_receipts, @{$self->_read_receipt_file($path)};
    }
    closedir $dh;

    #####
    # Cache all records. Not all records are for files from this run.
    # Probably none of the records are relevant to this run.
    #
    foreach my $receipt (@sample_receipts) {
      my ($file_name, $h) = $self->_parse_receipt($receipt);
      #####
      # We expect one record per file. If there are multiple records, they
      # should be identical.
      #
      if ($cache->{$file_name}) {
        my $m = "for $file_name record in one of the receipts in $dir";
        if ($h->{'sample'} ne $cache->{$file_name}->{'sample'}) {
          $self->logcroak("Mismatching sample names $m");
        }
        if ($h->{'flag'} != $cache->{$file_name}->{'flag'}) {
          $self->logcroak("Mismatching flags $m");
	}
        $self->logwarn("Duplicate record $m");
      } else {
        $cache->{$file_name} = $h;
      }
    }
  }

  return $cache;
}

sub _read_receipt_file {
  my ($self, $path) = @_;

  my @receipts = ();
  if ($path =~ /$RECIEPT_FILE_NAME_PATTERN/smx) {
    $self->debug("Reading receipt file $path");
    # slurp is no good for capturing errors. If something went
    # wrong, the @receipts list will be empty.
    @receipts = slurp q[-|], 'bzcat', $path;
    if (@receipts) {
      my $header = shift @receipts; # remove header row
      ($header =~ /\ABucket[ ]key/xms) or
        $self->logwarn("Unexpected header $header in $path");
      @receipts or $self->logwarn("Only header in $path");
    } else {
      $self->logwarn("Failed to read $path");
    }
  } else {
    $self->logwarn("Skipped $path - not a receipt?");
  }

  return \@receipts;
}

sub _parse_receipt {
  my ($self, $receipt) = @_;
  ##no critic (BuiltinFunctions::ProhibitComplexMappings)
  ##no critic (ControlStructures::ProhibitMutatingListFunctions)
  my @sample_data = grep { $_ ne q[] }
                    map  { s/\A\s//smxg; $_ }
                    map  { s/\s\Z//smxg; $_ }
                    split /\t/smx, $receipt;
  ##use critic
  (@sample_data == $RECIEPT_NUM_COLUMNS) or
    (@sample_data == ($RECIEPT_NUM_COLUMNS - 1)) or $self->logcroak(
    "Missing columns or data in '$receipt'");
  my ($sample, $file_name) = split /\//smx, $sample_data[0];
    ($sample && $file_name) or $self->logcroak(
    "Failed to get sample and file name from '$receipt'");

  my $flag = (($sample_data[$RECIEPT_NUM_COLUMNS - 1] || q[]) eq
               $RECIEPT_RECEIVED_FLAG) ? 1 : 0;

  return ($file_name, {sample => $sample, flag => $flag});
}

sub _received_by_customer {
  my ($self, $product) = @_;

  my $received = 0;
  my $file_name = $product->file_name(ext => $self->file_extension);
  my $receipt = $self->_cached_receipts->{$file_name};
  my $desc = $product->composition->freeze();

  if ($receipt) {
    my $sample_name = $product->lims->sample_supplier_name();
    $sample_name or $self->logcroak("Product $desc: missing supplier name");
    ($sample_name eq $receipt->{'sample'}) or
      $self->logcroak(sprintf
       'Our supplier sample name %s for file %s product %s ' .
       'differes from acknowledged sample name %s',
       $sample_name, $file_name, $desc, $receipt->{'sample'});
    $received = $receipt->{'flag'};
  } else {
    $self->logwarn("No receipt for file $file_name, product $desc");
  }

  return $received;
}

sub _passed_mqc {
  my ($self, $product) = @_;

  #####
  # This method will be called for products that are received
  # by the customer. It ensures that when the QC outcome matters,
  # it has not been reset after sending the product to the customer.
  #
  my $passed = 1;
  if ($self->qc_outcome_matters($product, 's3')) {
    my $desc = $product->composition->freeze();
    my $libqc_obj = $product->final_libqc_obj($self->qc_schema);
    $libqc_obj or $self->logcroak("Product $desc - not final lib QC outcome");
    $passed = $libqc_obj->is_accepted;
    $passed or $self->logwarn("Product $desc did not pass QC");
  }

  return $passed;
}

sub _failed_mqc {
  my ($self, $product) = @_;

  my $failed = 0;
  my $desc = $product->composition->freeze();

  my @seqqc = $product->final_seqqc_objs($self->qc_schema);
  @seqqc or $self->logcroak("Product $desc - not all final seq QC outcomes");

  $failed = any { $_->is_rejected }  @seqqc;
  if (!$failed) {
    my $libqc_obj = $product->final_libqc_obj($self->qc_schema);
    $libqc_obj or $self->logcroak("Product $desc - not final lib QC outcome");
    $failed = $libqc_obj->is_rejected;
  }
  $self->logwarn(sprintf 'Product %s %s QC',
                 $desc, $failed ? 'failed' : 'did not fail' );

  return $failed;
}

sub _saved4topup {
  my ($self, $product) = @_;

  my $saved = 0;
  my $desc = $product->composition->freeze();

  my $cache_dir = $self->merge_component_cache_dir($product);
  if ($cache_dir) {
    my $file_path = catfile($cache_dir,
      $product->file_name(ext => $self->file_extension));
    if (-f $file_path) {
      $saved = 1;
    }
    $self->logwarn(sprintf 'Product %s: cached file %s %sfound',
                   $desc, $file_path, $saved ? q[] : 'not ');
  } else {
    $self->logwarn("Product $desc: product cache directory not configured");
  }

  return $saved;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Readonly

=item Perl6::Slurp

=item English

=item List::MoreUtils

=item File::Spec::Functions

=item npg_pipeline::product::cache_merge

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 GRL

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
