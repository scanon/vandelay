#!/usr/bin/env perl

use JSON;
use Data::Dumper;
use Bio::KBase::HandleService;
use Bio::KBase::workspace::ScriptHelpers qw(get_ws_client workspace getObjectRef parseObjectMeta parseWorkspaceMeta printObjectInfo);
use Try::Tiny;
my $src=$ARGV[0];
my $ews=$ARGV[1];
my $wd=$ARGV[2];
use strict;
my $debug=0;

my $hs="kbhs";

#
#print "$oldws $newws\n";
my $hsrc = Bio::KBase::HandleService->new("https://".$src."/services/handle_service");

#
my $srcws = get_ws_client("https://".$src."/services/ws") or die "Unable to connect to $src ws";

my %handles;
my %refmap;
# old ws to new ws
my %mapws;

my %oldobj;
my %ws2id;

# list of referenced objects
my %references;

# Exist in target
my %exist;

-e $wd or mkdir $wd;
-e "$wd/data" or mkdir "$wd/data";
-e "$wd/refs" or mkdir "$wd/refs";
chdir $wd or die "Unable to cd";
load_ckpt("./kbei.ckpt");


# export/cache objects from WS
export_workspace($ews);

# Build map of src objects
print "Scanning exported objects\n";
scan_objects();


open CP,"> ./kbei.ckpt" or die "Unable to open ckpt";
print CP to_json({'handles'=>\%handles,'references'=>\%references,'objects'=>\%oldobj},{pretty=>1});
close CP;
print "Scan for missing references\n";
get_references();

print "Download handle data\n";
get_handle_files();


exit;

sub load_ckpt {
  my $f=shift;
  if ( -e "./kbei.ckpt"){
    print "Loading checkpoint\n";
    open CP,"./kbei.ckpt";
    my $js;
    while(my $line=<CP>){
      $js.=$line;
    }
    close CP;
    my $o=decode_json($js);
    %references=%{$o->{references}};
    %handles=%{$o->{handles}};
    %oldobj=%{$o->{objects}};
  }
}

# Scan files to get references and hash references
sub scan_objects {
  print "scaning\n";
  open(L,"find data -type f|");
  while(my $file=<L>){
    chomp $file;
    my ($data,$obj_byid)=split /\//,$file;
    print " - id: $obj_byid file:$file\n";
    process_object($file); # unless defined $references{$obj_byid};  
  }
  close L;
}

sub read_file {
  my $file=shift;
  my $data='';
  open(F,$file) or die "Unable to open $file\n";
  while(my $line=<F>){
     $data.=$line;
  }
  return $data;
}

sub process_object {
  my $file=shift;
  my $ishandle=0;

  my ($ddir,$wsname,$name,$wsid,$oid,$ver)=split /[\/~]/,$file;
  my $tobj_byid=join "/",($wsid,$oid,$ver);
  print "process: $tobj_byid\n";
  return if defined $oldobj{"$tobj_byid"};
  open(F,$file) or die "Unable to open object $file\n";
  my $json='';
  while(my $line=<F>){
     $json.=$line;
     $ishandle=1 if $line=~/"handle".*:/;
  }
  close F;
  my $objdata=decode_json $json;
  my ($objid,$name,$type,$sdate,$ver,$saveby,$wsid,$workspace,$chsum,$size,$meta)=@{$objdata->{info}};
  print "debug: $wsid $workspace\n";
  $ws2id{$workspace}=$wsid;
  my $obj_byname="$workspace/$name";
  my $obj_byid="$wsid/$objid/$ver";
  print "Processing $obj_byid\n";;
  my $wobj={'id'=>$objid,'file'=>$file,'name'=>$name,
	'type'=>$type,'ws'=>$workspace,'wsid'=>$wsid,
        'fname'=>$obj_byname, 'fid'=>$obj_byid};
  #process_object($wobj) unless defined $references{$obj_byname};  
  walk_object($objdata->{data},$wobj->{fid});
  $wobj->{handleobj}=$ishandle;
  print "$obj_byname $obj_byid h=$ishandle\n";
  $oldobj{"$obj_byname"}=$wobj;
  $oldobj{"$obj_byid"}=$wobj;
  return $wobj;
}

# Update handles
sub update_handle_object {
  my $o=shift;

  if (ref($o) eq "HASH"){
    if (defined $o->{hid}){
       # fix
       $o=$handles{$o->{hid}}->{new};
       return $o;
    }
    else{
      for my $so (keys %{$o}){
         $o->{$so}=update_handle_object($o->{$so});
      }
    }

  }
  elsif (ref($o) eq "ARRAY"){
    for my $so (@{$o}){
        update_handle_object($so);
    }
  }
  return $o;
 
}

sub walk_object {
  my $o=shift;
  my $me=shift;
  my @list;

  if (ref($o) eq "HASH"){
    if (defined $o->{hid}){
       $handles{$o->{hid}}=$o;
    }
    else{
      for my $so (keys %{$o}){
         if ($so=~/.ref/ && ($o->{$so})!=~/\~\// && $o->{$so}=~/[0-9]+\/[0-9]+\/[0-9]+/){
           my ($rws,$robj,$rver)=split /\//,$o->{$so};
           my $obj="$rws/$robj/$rver";
           if (! defined $references{$obj}){
             #print "reference: $so $rws $robj $rver by $me\n";
             $references{$obj}=$me;
           }
         }
         push @list,walk_object($o->{$so},$me);
      }
    }

  }
  elsif (ref($o) eq "ARRAY"){
    for my $so (@{$o}){
        push @list,walk_object($so,$me);
    }
  }
  return @list;
 
}

sub get_handle_files {
   #print Dumper(%handles);
   for my $hid (keys %handles){
     my $h=$handles{$hid};
     my $out=$hs."/".$h->{hid};
     #if (defined $files{$h->{file_name}}){
     #  print "Repeat\n";
     #  print Dumper($h);
     #  print Dumper($files{$h->{file_name}});
     #}
     #$files{$h->{file_name}}=$h;
     if (! -e $out){
       print "Downloading $out\n";
       my $rv  = $hsrc->download($h, $out);
     }
     my $out=$hs."/".$h->{hid}.".meta";
     if (! -e $out){
       print "Downloading $out\n";
       my $rv  = $hsrc->download_metadata($h, $out);
     }
     else{
       print "Skipping $hid $h->{file_name}\n";
     }
  }
}


sub get_references {
  for my $obj (keys %references){
    if (! defined $oldobj{$obj}){
#ObjectIdentity is a reference to a hash where the following keys are defined:
#        workspace has a value which is a Workspace.ws_name
#        wsid has a value which is a Workspace.ws_id
#        name has a value which is a Workspace.obj_name
#        objid has a value which is a Workspace.obj_id
#        ver has a value which is a Workspace.obj_ver
#        ref has a value which is a Workspace.obj_ref
       print " - Checking $obj\n";
       my ($wsid,$oid,$ver)=split /\//,$obj;
       my $ref_chain=[{'wsid'=>$wsid,'objid'=>$oid,'ver'=>$ver}];
       my $done=0;
       try {
          my $objdata=$srcws->get_objects($ref_chain);
          serialize_object($objdata->[0],"./refs/");
          $done=1;
       }
       catch {
            print " - Unable to get $obj ".substr($_,0,90)."...\n";
            print " - Trying get_referenced_object\n";
       };
       next if $done;
       my $robj=$obj;
       while (defined $references{$robj}){
         print "Ref chain $obj refby:$references{$obj}\n";
         my ($wsid,$oid,$ver)=split /\//,$references{$obj};
         unshift @{$ref_chain},{'wsid'=>$wsid,'objid'=>$oid,'ver'=>$ver};
         $robj=$references{$robj};
       }
       my $refs=scalar @{$ref_chain};
       print "Refs: $refs\n";
       try {
          my $objdata=$srcws->get_referenced_objects([$ref_chain]);
          serialize_object($objdata->[0],"./refs/");
       }
       catch {
            print Dumper($ref_chain);
            print "Unable to get $obj ".substr($_,0,90)."...\n";
       }
    }
  }
}

sub serialize_object {
  my $data=shift;
  my $file=shift;

  my ($oid,$name,$type,$save_data,$version,$saved_by,$wsid,$workspace,$chsum,$size,$meta)=@{$data->{info}};
  if (-d $file){
    $file.=join "~",($workspace,$name,$wsid,$oid,$version);
  }
  return if -e $file;
  print " - serialize reference: $oid, $name, $type, $workspace, $wsid\n";
  open(O, "> $file") or die "Unable to open $file";
  print O to_json($data,{ ascii => 1, pretty => 1 } );
  close O;
}

#ObjectData is a reference to a hash where the following keys are defined:
#        data has a value which is an UnspecifiedObject, which can hold any non-null object
#        info has a value which is a Workspace.object_info
#        provenance has a value which is a reference to a list where each element is a Workspace.ProvenanceAction
#        creator has a value which is a Workspace.username
#        created has a value which is a Workspace.timestamp
#        refs has a value which is a reference to a list where each element is a Workspace.obj_ref
#object_info is a reference to a list containing 11 items:
#        0: (objid) a Workspace.obj_id
#        1: (name) a Workspace.obj_name
#        2: (type) a Workspace.type_string
#        3: (save_date) a Workspace.timestamp
#        4: (version) an int
#        5: (saved_by) a Workspace.username
#        6: (wsid) a Workspace.ws_id
#        7: (workspace) a Workspace.ws_name
#        8: (chsum) a string
#        9: (size) an int
#        10: (meta) a Workspace.usermeta
#}


sub  export_workspace {
  my $ws=shift;
  my $x=$srcws->list_workspace_info({});
  my %srcwsl;
  map {$srcwsl{$_->[1]}=$_->[0];} @{$x};
  # List the export workspace
  my $x=$srcws->list_objects({'workspaces'=>[$ws]});
  for my $obj (@{$x}){
    my ($oid,$name,$type,$sdate,$ver,$saveby,$wsid,$workspace,$chsum,$size,$meta)=@{$obj};
    my $file="./data/".join "~",($ws,$name,$wsid,$oid,$ver);
    print "$file\n";
    if (! -e $file){
      my $objref=[{'wsid'=>$wsid,'objid'=>$oid,'ver'=>$ver}];
      #print Dumper($objref);
      eval {
      my $objdata=$srcws->get_objects($objref);
      serialize_object($objdata->[0],$file) unless -e $file;
      }
    }
  }
}

#  obj_id objid - the numerical id of the object.
#                   obj_name name - the name of the object.
#                   type_string type - the type of the object.
#                   timestamp save_date - the save date of the object.
#                   obj_ver ver - the version of the object.
#                   username saved_by - the user that saved or copied the object.
#                   ws_id wsid - the workspace containing the object.
#                   ws_name workspace - the workspace containing the object.
#                   string chsum - the md5 checksum of the object.
#                   int size - the size of the object in bytes.
#                   usermeta meta - arbitrary user-supplied metadata about
#                           the object.
