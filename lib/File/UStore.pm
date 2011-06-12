package File::UStore;

use 5.006;
use strict;
use warnings;

=head1 NAME

File::UStore - Perl extension to store files  on a filesystem using a non-hash UUID(Universally Unique Identifier) based randomised storage with depth of storage options.

=head1 VERSION

Version 0.04

=cut

our $VERSION = '0.04';


use UUID;
use File::Copy;
use File::Path;
use File::Spec;

require Exporter;
use AutoLoader qw(AUTOLOAD);

our @ISA = qw(Exporter);

our %EXPORT_TAGS = (
    'all' => [
        qw(

            )
    ]
);

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw( );

=head1 SYNOPSIS

  use File::UStore;
  my $store = new File::UStore( path => "/home/shantanu/.teststore", 
                              prefix => "prefix_",
                              depth  => 5
                            );
  
  open( my $FH, "foo.pl" ) or die "Unable to open file ";
  # Add a file in the store
  my $id = $store->add(*$FH);

  # Get file handle from id. this might not work if you have too wierd a storage scheme.
  my $handle = $store->get($id);
  print <$handle>;

  # Return the filesystem location of an id
  my $location = $store->getPath($id);

  # Remove a file by its id from the store
  $store->remove("7d4d873e-4bf4-41a5-8696-fd6232f7bdda");

=head1 DESCRIPTION

File::UStore is a perl library based on File::HStore to store files on a filesystem using a UUID based randomised storage with folder depth control over storage.

File::UStore is a library that allows users to abstract file storage using a UUID based pointer instead of File Hashes to store the file. This is a critical feature for code which requires even duplicate files to get a unique identifier each time they added to a store. A Hash Storage on the other hand will not allow a file to be duplicated if it is stored multiple time in the store. This can cause issues in cases where a files may be deleted regularly as there would be no way of knowing if a second process is still using the file which the first process might be about to delete.

The  current version  uses UUID Module to generate universally unique identifiers everytime a file is to be stored.

The Module also provides a option to choose the folder depth at which a file is stored. This can be changed from the default value of 3. Increasing depth is advisable 
if the store might contain a large number of files in your use case. This will help to avoid having a too large number of files in any single folder.

=cut

=head1 SUBROUTINES/METHODS

The object  oriented interface to C<File::UStore> is  described in this
section.  

The following methods are provided:

=head2 new

=item $store = File::UStore->new( path => "/home/shantanu/.teststore", prefix => "prefix_", depth  => 5 );

This constructor  returns a new C<File::UStore>  object encapsulating a
specific store. The path specifies  where the UStore is located on the
filesystem.  If the  path  is  not specified,  the  path ~/.ustore  is
used. The $prefix is an extension to specify the prefix appended before 
unique file name.

=cut

sub new {

    my ( $this,%params) = @_;
    
    my $class = ref($this) || $this;
    my $self = {};
    bless $self, $class;

    if ( exists($params{path}) ) {
        $self->{path} = $params{path};
    }
    else {
        $self->{path} = "~/.ustore";
    }

    if ( exists($params{prefix}) ) {
        $self->{prefix} = $params{prefix};
    }
    else {
        $self->{prefix} = "";
    }

    if ( exists($params{depth}) ) {
        $self->{depth} = $params{depth};
        $self->{depth} = 35 if ($params{depth}>35);
        
    }
    else {
        $self->{depth} = 3;
    }

    if ( !( -e $self->{path} ) ) {
        mkdir( $self->{path} )
            or die "Unable to create directory : $self->{path}";
    }


    return $self;
}

=head2 add

=item $store->add($filename)

The $filename is the file to be added in the  store. The return value
is the id ($id) of the $filename stored. From this point on the user 
will only be able to refer to this file using the id.
Return undef on error. 

=cut

sub add {

    my ( $self, $file) = @_;

    my ($uuid,$uuidString); 
    UUID::generate($uuid);
    UUID::unparse($uuid, $uuidString);

    my $SSubDir;
    my @tempstr = split (//,$uuidString);
    my @dirtree;
    for (1 .. $self->{depth}){
      push @dirtree , shift @tempstr;    
    }
    $SSubDir = File::Spec->catdir($self->{path} , @dirtree);

    if ( !( -e $SSubDir ) ) {

        mkpath($SSubDir) or die "Unable to create subdirectories $SSubDir in the store";
    }

    my $destStoredFile = File::Spec->catfile($SSubDir , $self->{prefix}.$uuidString);

        copy( $file, $destStoredFile )
        or die "Unable to copy file into ustore as $destStoredFile";

    return $uuidString;
}

=head2 remove

=item $store->remove($id)

The $id is the file to be removed from the store. 

Return false on success and undef on error.


=cut

sub remove {

    my ( $self, $id ) = @_;

    my $destStoredFile;

    if ( !( defined($id) ) ) { return undef; }

    my $SSubDir;
    my @tempstr = split (//,$id);
    my @dirtree;
    for (1 .. $self->{depth}){
      push @dirtree , shift @tempstr;    
    }
    $SSubDir = File::Spec->catdir($self->{path} , @dirtree);

    $destStoredFile = File::Spec->catfile($SSubDir , $self->{prefix}.$id);

    if ( -e $destStoredFile ) {

            unlink($destStoredFile) or return undef;

    }
    else {
        return;
    }

}

=head2 get

=item $store->get($id)

Return the file handle of the file from its id.

Return undef on error.

=cut

sub get {

    my ( $self, $id ) = @_;

    my $destStoredFile;

    my $SSubDir;
    my @tempstr = split (//,$id);
    my @dirtree;
    for (1 .. $self->{depth}){
      push @dirtree , shift @tempstr;    
    }
    $SSubDir = File::Spec->catdir($self->{path} , @dirtree);

    $destStoredFile = File::Spec->catfile($SSubDir , $self->{prefix}.$id);

    if ( -e $destStoredFile ) {
        open ( my $fh, '<', $destStoredFile );
	return *$fh;
    } else {
	return;
    }
}

=head2 getPath

=item $store->getPath($id)

Return the filesystem location of the file from its id.

Return undef on error.

=cut

sub getPath {

    my ( $self, $id ) = @_;

    my $destStoredFile;

    my $SSubDir;
    my @tempstr = split (//,$id);
    my @dirtree;
    for (1 .. $self->{depth}){
      push @dirtree , shift @tempstr;    
    }
    $SSubDir = File::Spec->catdir($self->{path} , @dirtree);

    $destStoredFile = File::Spec->catfile($SSubDir , $self->{prefix}.$id);

    if ( -e $destStoredFile ) {
	return $destStoredFile;
    } else {
	return;
    }
}

# local Function
sub _printPath {
    my ($self) = @_;

    return $self->{path};

}

1;
__END__


=head1 SEE ALSO

An Analysis of Compare-by-hash - for reasons why a UUID based storage may
be preferred over hash based solution in certain cases.
http://www.usenix.org/events/hotos03/tech/full_papers/henson/henson.pdf

=head1 USE CASE FOR THIS MODULE IN LIEU OF A HASH BASED STORAGE

File::HStore is a similar module that
provides File Hash based storage. However due to the nature of File
Hashing, File::HStore doesn't allow duplicates. If the same file is
stored a second time using File::HStore it transparently returns the
same hash it had returned last time as an id without adding any new 
file in storage due to inherent character of hash based storage, while 
this is useful if a user doesn't want any duplicates occurring in a
storage, this apparently trivial difference is risky in the use case
where two processes upload a duplicate file to the store and both
processes want to do file handling simultaneously, only one of the 
processes will be able to get a lock(deletion,manipulation etc.) on 
the file at a time and if the first process deletes the file referred 
to by its ID, the second process will never know what happened to the 
file it added. 

Hence to serve this unique use case I wrote this module for a UUID 
based storage solution which is not hostage to auto de-duping features 
of HStore. This module returns a unique file id each time a file is 
uploaded even if its a duplicate of existing file previously uploaded. 
This allows multiple processes handling data from a common file dump to 
access the same file. This module also expands on the Hstore to allow 
the user flexibility of choosing the depth of storage to optimize the
performance for the users application. Depth of storage allows users
to make the balance between average "number of files in a folder"
and folder depth.

=head1 AUTHOR

Shantanu Bhadoria, C<< <shantanu (dot comes here) bhadoria at gmail dot com> >> L<http://www.shantanubhadoria.com>.

=head1 BUGS

Please report any bugs or feature requests to C<bug-file-ustore at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=File-UStore>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc File::UStore

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-UStore>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/File-UStore>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/File-UStore>

=item * Search CPAN

L<http://search.cpan.org/dist/File-UStore/>

=back

=head1 ACKNOWLEDGEMENTS

Thanks to Alexandre Dulaunoy for the excellent File::HStore module which along with my own special need provided the idea behind this module.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 Shantanu Bhadoria  

C<< <shantanu (dot comes here) bhadoria at gmail dot com> >> L<http://www.shantanubhadoria.com>

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=head1 Dependencies 

UUID

File::Copy

File::Path

File::Spec

=cut
