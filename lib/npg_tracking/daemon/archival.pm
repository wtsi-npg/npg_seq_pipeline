#########
# Author:        Marina Gourtovaia
# Maintainer:    $Author: mg8 $
# Created:       18 December 2009
# Last Modified: $Date: 2014-11-26 14:28:42 +0000 (Wed, 26 Nov 2014) $
# Id:            $Id: archival.pm 18739 2014-11-26 14:28:42Z mg8 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/lib/npg_tracking/daemon/archival.pm $
#

package npg_tracking::daemon::archival;

use Moose;
use English qw{-no_match_vars};
use Readonly;

extends 'npg_tracking::daemon';

Readonly::Scalar our $VERSION => do { my ($r) = q$Revision: 18739 $ =~ /(\d+)/smx; $r; };

Readonly::Scalar our $SCRIPT_NAME => q[npg_pipeline_archival_runner];

override '_build_hosts' => sub { return ['sf2-farm-srv1','sf2-farm-srv2']; };
override 'daemon_name'  => sub { return $SCRIPT_NAME; };
override 'command'      => sub { return $SCRIPT_NAME; };

no Moose;

1;
__END__

=head1 NAME

npg_tracking::daemon::archival

=head1 VERSION

$LastChangedRevision: 18739 $

=head1 SYNOPSIS

=head1 DESCRIPTION

Defenition for the daemon for the archival pipeline.

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item English

=item npg_tracking::daemon

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Author: Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2009 GRL, by Marina Gourtovaia

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




