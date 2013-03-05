use 5.004;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'File::UStore',
    VERSION_FROM      => 'lib/File/UStore.pm', # finds $VERSION
    PREREQ_PM         => { 'UUID'       => '0',
			   'File::Copy' => '0',
			   'File::Spec' => '0',
			   'Test::More' => '0',
			   'File::Path' => '0' }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/File/UStore.pm', # retrieve abstract from module
       LICENSE	      => 'perl',
       AUTHOR         => 'Shantanu Bhadoria <shantanu.bhadoria@gmail.com>') : ()),
    META_MERGE => {
        resources => {
            repository => 'https://github.com/dwimperl/Task-DWIM',
        },
    },
);
