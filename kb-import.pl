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

my $hs="kbhs";

#
#print "$oldws $newws\n";
my $hdst = Bio::KBase::HandleService->new("http://".$dst."/services/handleservice");

#
my $dstws = get_ws_client("http://".$dst."/services/ws") or die "Unable to connect to $dst ws";

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

chdir $wd or die "Unable to cd";
load_ckpt("./kbei.ckpt");


## export/cache objects from WS
#export_workspace($ews);

## Build map of src objects
#print "Scanning exported objects\n";
#scan_objects();


#open CP,"> ./kbei.ckpt" or die "Unable to open ckpt";
#print CP to_json({'handles'=>\%handles,'references'=>\%references,'objects'=>\%oldobj},{pretty=>1});
#close CP;
#print "Scan for missing references\n";
#get_references();

#print "Download handle data\n";
#get_handle_files();

# Build a list of workspaces
my %wsl;
my $x=$dstws->list_workspace_info({});
map {$wsl{$_->[1]}=$_->[0];} @{$x};

for my $o (keys %oldobj){
  $ws2id{$oldobj{$o}->{ws}}=$oldobj{$o}->{wsid};
}

print "Scanning target workspaces for references\n";
for my $ws (sort keys %ws2id){
  if (! defined $wsl{$ws}){
    print "Create $ws\n";
    $dstws->create_workspace({'workspace'=>$ws}) || exit;
  }
  my $newws=$wsl{$ws};
  #$_=`ws-workspace $ws|tail -1` or die "workspace doesn't exist";
  #my ($newws,$name,$owner)=split;
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
#        $handles{$hid}->{new}=hs_upload($hdst,$handles{$hid});
} 

print "- Loading workspace object\n";
my $loaded=0;
for my $object (sort keys %oldobj){
  next if $object=~/[0-9]+\/[0-9]+/;
  next if defined $exist{$object};
  print "Load $object\n";
  my $wobj=$oldobj{$object};
  # Read in data
  my $file=$oldobj{$object}->{file};
  my ($ws,$obj)=split /\//;
  my $objdata;
  my $json=read_file($file);
  if ($wobj->{handleobj} == 1){
    # Create a new object
    print "Updating Handle Object $wobj->{name}\n";
    $objdata=decode_json($json); 
    #my $new=update_handle_object(decode_json($json));
    my $new=update_handle_object($objdata->{data});
    #print Dumper($new);
    #open(TMP,"> tmp.out");
    #print TMP to_json($new);
    #close TMP;
  }
  else{
    # could change this to just do references known to be in the object
    for my $oldref (keys %refmap) {
      my $newref=$refmap{$oldref};
      $json=~s |$oldref[0-9]+|$newref|g;
    }
    $objdata=decode_json($json);
    #open(TMP,"> tmp.out");
    #print TMP $json;
    #close TMP;
  }
  #`ws-workspace $wobj->{ws}` or die "Unable to choose ws";
  #`ws-load $wobj->{type} $wobj->{name} tmp.out` or die "Failed to load";
  my $saveobj={'type'=>$wobj->{type},'name'=>$wobj->{name},'data'=>$objdata->{data},'meta'=>$objdata->{metaa},'provenance'=>undef};
  #open(O,">/tmp/d.out");
  #print O Dumper($saveobj);
  # close O;
  my $p={'workspace'=>$wobj->{ws}, 'objects'=>[$saveobj]};
  #next;
  try {
    print "Save $object\n" if $debug;
    my $result=$dstws->save_objects($p);
    $loaded++;
  }
  catch {
     print "Failed to save $wobj->{name}\n$_\n";#$_;
     #if ($wobj->{name}=~/rhodo/){
     #  print Dumper($p);
     #  open T, ">tmp";
     #  print T to_json($p->{objects}[0]->{data},{ ascii => 1, pretty => 1 } );
     #  exit;
     #}
  };

}
print "Loaded $loaded new objects\n";

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
  open(L,"find data -type f|");
  while(my $file=<L>){
    chomp $file;
    my ($data,$obj_byid)=split /\//,$file;
    print "id: $obj_byid file:$file\n";
    process_object($file); # unless defined $references{$obj_byid};  
    #print "$ws/$name $_\n";
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

#
#sub get_handle_files {
#   for my $hid (keys %handles){
#     my $h=$handles{$hid};
#     my $out=$hs."/".$h->{hid};
#     #if (defined $files{$h->{file_name}}){
#     #  print "Repeat\n";
#     #  print Dumper($h);
#     #  print Dumper($files{$h->{file_name}});
#     #}
#     #$files{$h->{file_name}}=$h;
#     if (! -e $out){
#       print "Downloading $out\n";
#       my $rv  = $hsrc->download($h, $out);
#     }
#     my $out=$hs."/".$h->{hid}.".meta";
#     if (! -e $out){
#       print "Downloading $out\n";
#       my $rv  = $hsrc->download_metadata($h, $out);
#     }
#     else{
#       print "Skipping $hid $h->{file_name}\n";
#     }
#  }
#}*/

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


#sub get_references {
#  for my $obj (keys %references){
#    my $file="./refs/".join "~",split /\//,$obj;
#    if (-e $file){
#        print "Skipping $obj..cached\n";
#        next;
#    }
#    next if -e $file;
#    if (! defined $oldobj{$obj}){
##ObjectIdentity is a reference to a hash where the following keys are defined:
##        workspace has a value which is a Workspace.ws_name
##        wsid has a value which is a Workspace.ws_id
##        name has a value which is a Workspace.obj_name
##        objid has a value which is a Workspace.obj_id
##        ver has a value which is a Workspace.obj_ver
##        ref has a value which is a Workspace.obj_ref
#       my ($wsid,$oid,$ver)=split /\//,$obj; 
#       my $ref_chain=[{'wsid'=>$wsid,'objid'=>$oid,'ver'=>$ver}];
#       my $robj=$obj;
#       while (defined $references{$robj}){
#         #print "Ref chain $obj refby:$references{$obj}\n";
#         my ($wsid,$oid,$ver)=split /\//,$references{$obj};
#         unshift @{$ref_chain},{'wsid'=>$wsid,'objid'=>$oid,'ver'=>$ver};
#         $robj=$references{$robj};
#       }
#       try {
#          my $objdata=$srcws->get_referenced_objects([$ref_chain]);
#          serialize_object($objdata->[0],$file);
#       }
#       catch {
#            #print Dumper($ref_chain);
#            print "Unable to get $obj ".substr($_,0,60)."...\n";
#       }
#    }
#  }
#}

sub serialize_object {
  my $data=shift;
  my $file=shift;

  my ($oid,$name,$type,$save_data,$version,$saved_by,$wsid,$workspace,$chsum,$size,$meta)=@{$data->{info}};
  print "reference: $oid, $name, $type, $workspace, $wsid\n";
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


#sub  export_workspace {
#  my $ws=shift;
#  my $x=$srcws->list_workspace_info({});
#  my %srcwsl;
#  map {$srcwsl{$_->[1]}=$_->[0];} @{$x};
#  # List the export workspace
#  my $x=$srcws->list_objects({'workspaces'=>[$ws]});
#  for my $obj (@{$x}){
#    my ($oid,$name,$type,$sdate,$ver,$saveby,$wsid,$workspace,$chsum,$size,$meta)=@{$obj};
#    my $file="./data/".join "~",($wsid,$oid,$ver);
#    print "$file\n";
#    if (! -e $file){
#      my $objref=[{'wsid'=>$wsid,'objid'=>$oid,'ver'=>$ver}];
#      my $objdata=$srcws->get_objects($objref);
#      serialize_object($objdata->[0],$file) unless -e $file;
#    }
#  }
#}

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
