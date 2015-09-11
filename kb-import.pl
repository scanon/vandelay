#!/usr/bin/env perl

use JSON;
use Data::Dumper;
use Bio::KBase::HandleService;
use Bio::KBase::workspace::ScriptHelpers qw(get_ws_client workspace getObjectRef parseObjectMeta parseWorkspaceMeta printObjectInfo);
use Try::Tiny;
my $dst=$ARGV[0];
my $wd=$ARGV[1];
use strict;
my $debug=0;
my $ignore_errors=1;
my $defaultws="KBaseReferences";

my $hs="kbhs";

#
my $hdst = Bio::KBase::HandleService->new("http://".$dst."/services/handleservice");

#
my $dstws = get_ws_client("http://".$dst."/services/ws") or die "Unable to connect to $dst ws";

my %handles;

# Maps an old reference to a new reference.
# The format is ##/##/##  ws/id/version.
my %refmap;

# old ws to new ws
my %mapws;

my %oldobj;
my %ws2id;

# oldids
# List of all oldids and the file that has the data
my %oldids;

# list of referenced objects
my %references;
my %refs;

# Exist in target
my %exist;

chdir $wd or die "Unable to cd";
load_ckpt("./kbei.ckpt");

# Scan the downloaded referenced objects
scan_ref_objects();

# Build a list of workspaces
my %ws_list;
my $x=$dstws->list_workspace_info({});
# A bit cryptic.  list_workspace returns an array of array references
# The 0th element is the id, the 1st element is the name
map {$ws_list{$_->[1]}=$_->[0];} @{$x};

# scan the exported objects and build up a map of old ids to names
for my $o (keys %oldobj){
  $ws2id{$oldobj{$o}->{ws}}=$oldobj{$o}->{wsid};
  $oldids{$o}=$oldobj{$o}->{file};
}

print "Scanning target workspaces for references\n";
# Go through the list of workspaces and look for objects
# that have already been uploaded.  Add that to the refmap
for my $ws (sort keys %ws2id){
  if (! defined $ws_list{$ws}){
    print "Create $ws\n";
    $dstws->create_workspace({'workspace'=>$ws}) || exit;
  }
  my $newws=$ws_list{$ws};
  print "$ws $newws\n";
  $mapws{$ws2id{$ws}}=$newws;
  my $x=$dstws->list_objects({'workspaces'=>[$ws]});
  for my $obj (@{$x}){
    my $i=$obj->[0];
    my $name=$obj->[1];
    my $v=$obj->[4];
    $exist{"$ws/$name"}=$v;
    # map old to new
    if (defined $oldobj{"$ws/$name"}){
      #print "map: $ws oldid=$src{$name} newid=$i ver=$v\n";
      my $oid=$oldobj{"$ws/$name"}->{id};
      my $oldref="$ws2id{$ws}/$oid/";
      my $newref="$newws/$i/$v";
      $refmap{$oldref}=$newref;
    }
  }
  close W;
}

print "Loading stage\n";
print "- Loading Handle data\n";
for my $hid (keys %handles){
    try {
      my $tmp=hs_upload($hdst,$handles{$hid});
      $handles{$hid}->{new}=$tmp;
    }
    catch {
      print STDERR "Upload of handle $hid failed\n";
    };
}

# Upload refernces

print "- Loading workspace object\n";
my $loaded=0;
for my $object (sort keys %oldobj){
  next if $object=~/[0-9]+\/[0-9]+/;
  next if defined $exist{$object};
  print "Load $object\n";
  my $wobj=$oldobj{$object};
  my $file=$oldobj{$object}->{file};
  my ($ws,$obj)=split /\//;
  load_object($file);

}
print "Loaded $loaded new objects\n";

exit;

# Load the object
sub load_object{
  my $file=shift;

  # Read in data
  print " - Loading $file\n";
  my $objdata;
  my $json=read_file($file);
  my $objdata=decode_json($json); 
  my ($objid,$name,$type,$sdate,$ver,$saveby,$wsid,$workspace,$chsum,$size,$meta)=@{$objdata->{info}};
  my $obj_byname="$workspace/$name";
  my $obj_byid="$wsid/$objid/$ver";
  my $wobj={'id'=>$objid,'file'=>$file,'name'=>$name,
        'type'=>$type,'ws'=>$workspace,'wsid'=>$wsid,
        'fname'=>$obj_byname, 'fid'=>$obj_byid};

  if (defined $objdata->{'extracted_ids'}->{'handle'}){
    # Create a new object
    print "Updating Handle Object $wobj->{name}\n";
    my $new=update_handle_object($objdata->{data});
  }
  else{
    # could change this to just do references known to be in the object
    $json=scan_references($json);
    # Maybe we should decode the object again?
    $objdata=decode_json($json); 
  }
  my $saveobj={'type'=>$wobj->{type},'name'=>$wobj->{name},'data'=>$objdata->{data},'meta'=>$objdata->{metaa},'provenance'=>undef};
  if ( ! defined $ws_list{$workspace}){
    print "Workspace $workspace doesn't exist.  Using default $defaultws\n";
    $workspace=$defaultws;
  }
  my $p={'workspace'=>$workspace, 'objects'=>[$saveobj]};
  try {
    print "Save $workspace/$name\n" if $debug;
    my $result=$dstws->save_objects($p);
    $loaded++;
    my ($objid,$name,$type,$sdate,$ver,$saveby,$wsid,$workspace,$chsum,$size,$meta)=@{$result->[0]};
    # The new objected id
    my $newobj_byid="$wsid/$objid/$ver";

    # Record the mapping
    $refmap{$obj_byid}=$newobj_byid;

    open(F,'>'.$file.'~'.$dst) or die "Unable to open output file";
    print F to_json($result->[0],{ ascii => 1, pretty => 1 } );
    close F;
  }
  catch {
     print "Failed to save $wobj->{name}\n";
     print "   ".substr($_,1,2000)."...\n";
     open T, ">tmp";
     print T to_json($p->{objects}[0]->{data},{ ascii => 1, pretty => 1 } );
     close T;
     exit if $ignore_errors eq 0;
     #if ($wobj->{name}=~/rhodo/){
     #  print Dumper($p);
       exit;
     #}
  };

}

# Search the object data for references
#
sub scan_references {
  my $json=shift;

  for my $oldref (keys %oldids){
    if ($json=~/$oldref/ && ! defined $refmap{$oldref}){
      print "Missing reference: $oldref.  Loading.\n";
      load_object($oldids{$oldref});
    }
  }
  for my $oldref (keys %refmap) {
     my $newref=$refmap{$oldref};
     if ($oldref=~/[0-9]$/){  # Versioned reference
       #print "   D: Versioned substituting $oldref with $newref\n";
       $json=~s |$oldref|$newref|g;
     }
     else { # Non-vesioned reference
       #print "   D: Non-versioned substituting $oldref with $newref\n";
       $json=~s |$oldref[0-9]+|$newref|g;
     }
  }
  return $json;
}

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

# Scan referenced objects and build up a map
# Also keep track of any objects that have already been uploaded.
sub scan_ref_objects {
  open(L,"find refs -type f|");
  while(my $file=<L>){
    chomp $file;
    my ($data,$workspace,$name,$wsid,$objid,$ver,$host)=split /[\/~]/,$file;
    my $obj_byid="$wsid/$objid/$ver";
    if (defined $host && $host eq $dst){
      print " - Already uploaded $obj_byid to $dst\n";
      my $json=read_file($file);
      my $data=decode_json($json);
      my ($objid,$name,$type,$sdate,$ver,$saveby,$wsid,$workspace,$chsum,$size,$meta)=@{$data};
      my $new_byid="$wsid/$objid/$ver";
      $refmap{$obj_byid}=$new_byid;
    }
    else{
      my $obj_byid="$wsid/$objid/$ver";
      $oldids{$obj_byid}=$file;
    }
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

  my ($ddir,$workspace,$name,$wsid,$oid,$ver)=split /[\/~]/,$file;
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


sub hs_upload {
  my $hdst=shift;
  my $h=shift;
  my $json;

  my $fn=$h->{hid};
  my $hf="$hs/$fn.$dst";
  if ( ! -e "$hf" ){
    my $filename=$h->{file_name};
    $filename="tmpfile.".$h->{type} if ! defined $filename;
    unlink $filename if -e $filename;
    symlink("$hs/$fn",$filename) or die "Unable to create symlink $fn";
    print "Upload $fn\n";
    my $handle=$hdst->upload($filename);
    my $out=$hdst->upload_metadata($handle,$hs."/".$h->{hid}.".meta");
    $json=to_json($handle);
    open(HF,"> $hf");
    print HF $json;
    close HF;
    unlink $h->{file_name};
    #print Dumper($h);  
    return $handle;
  } 
  else{
    print "Already uploaded $h->{hid}\n";
    open(HF,"$hf");
    $json=<HF>;
    close HF;
    return decode_json($json);
  }
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
