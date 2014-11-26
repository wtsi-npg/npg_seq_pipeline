################
# Created by: ajb
# Maintainer: $Author: mg8 $ 
# Created on: 2009-04-08
# Last Changed: $Date: 2014-11-17 15:52:45 +0000 (Mon, 17 Nov 2014) $
# Id: $Id: Build.PL 18722 2014-11-17 15:52:45Z mg8 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/Build.PL $

use strict;
use warnings;
use Module::Build;

our $VERSION = do { my @r = (q$Revision: 18722 $ =~ /\d+/mxgs); sprintf '%d.'.'%03d' x $#r, @r };

my $class = Module::Build->subclass(code => <<'EOF');

  sub process_data_files {
    my $self = shift;
    `find data/config_files -type f | grep -v .svn | cpio -pdv --quiet blib`;
    return;
  }
EOF

my $builder = $class->new(
         module_name         => 'npg_pipeline',
         dist_author         => q(NPG team <npg@sanger.ac.uk>),
         dist_version        => $VERSION,
         dist_abstract       => q[WTSI pluggable pipeline for Illumina sequencing post processing and QC],
         license             => 'gpl',

         requires            => {
          'autodie'               => 0,
          'Carp'                  => 1.04,
          'Cwd'                   => 0,
          'Config::Any'           => 0,
          'Date::Format'          => 0,
          'English'               => 1.02,
          'File::Basename'        => 0,
          'File::Copy'            => 0,
          'File::Find'            => 0,
          'File::Path'            => 0,
          'File::Slurp'           => 0,
          'File::Spec'            => 0,
          'File::Spec::Functions' => 0,
          'FindBin'               => 0,
          'Getopt::Long'          => '2.37',
          'JSON'                  => 0,
          'lib'                   => 0,
          'List::MoreUtils'       => 0,
          'List::Util'            => 0,
          'Moose'                        => 0.93,
          'Moose::Meta::Class'           => 0,
          'Moose::Role'                  => 0,
          'Moose::Util::TypeConstraints' => 0,
          'MooseX::AttributeCloner'      => 0.17,
          'MooseX::ClassAttribute'       => 0,
          'MooseX::Getopt'               => 0,
          'Perl6::Slurp'           => 0,
          'POSIX'                  => 0,
          'Readonly'               => 1.03,
          'strict'                 => 1.03,
          'Sys::Filesystem::MountPoint' => 0,
          'Try::Tiny'              => 0,
          'warnings'               => 1.05,
          'XML::LibXML'            => 0,

          'npg_common::roles::log'                          => 0,
          'npg_common::roles::run::lane::file_names'        => 0,
          'npg_common::roles::software_location'            => 0,         
          'npg_qc::autoqc::autoqc'                          => 0,
          'npg_qc::autoqc::qc_store'                        => 0,
          'npg_tracking::data::reference::find'             => 0,
          'npg_tracking::data::reference::list'             => 0,
          'npg_tracking::daemon'                            => 0,
          'npg_tracking::glossary::lane'                    => 0,
          'npg_tracking::glossary::run'                     => 0,
          'npg_tracking::glossary::tag'                     => 0,
          'npg_tracking::illumina::run::folder'             => 0,
          'npg_tracking::illumina::run::folder::validation' => 0,
          'npg_tracking::illumina::run::long_info'          => 0,
          'npg_tracking::illumina::run::short_info'         => 0,
          'npg_tracking::util::types'                       => 0,
          'st::api::lims'                                   => 0,
          'npg::api::request'                               => 0,
          'npg::api::run'                                   => 0,
          'npg::api::run_status_dict'                       => 0,
          'npg::samplesheet'                                => 0,
         },

         build_requires      => {
          'Digest::MD5'                 => 0,
          'File::Temp'                  => 0,
          'Sys::Hostname'               => 0,
          'Test::Deep'                  => 0,
          'Test::Exception'             => 0,
          'Test::Exception::LessClever' => 0,
          'Test::Differences'           => 0,
          'Test::More'                  => '0.8',
          'Test::Perl::Critic'          => 0,
          'YAML'                        => 0,
          'npg_testing::db'             => 0,
         },

         configure_requires => {
          'Readonly'                    => 0,
          'Readonly::XS'                => 0,
         },
);

if ($builder->install_base()) {
  $builder->install_path('data' => join q{/}, $builder->install_base(), 'data');
  $builder->add_build_element('data');
} else {
  warn "WARNING: '--install_base' option is not given, 'data' element will not be installed\n\n";
}
$builder->create_build_script();
