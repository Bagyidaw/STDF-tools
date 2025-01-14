
use strict;
use warnings;


## Copyright Nyan 2024
## GPL v3


use IO::Uncompress::Gunzip qw($GunzipError);
use FindBin qw/$Bin/;
use lib "$Bin";
use Data::Dumper;

use DBI;
use Time::Piece;
use STDF::Parser;
use MIME::Base64 qw(encode_base64);
use File::Basename;

my $SQLITE_TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S";

if(@ARGV < 2) {
	print "$0 <STDF> <db>\n";
	exit;
}
my $file = $ARGV[0];
my $dbfile = $ARGV[1];
my $fh;
if($file =~ /\.gz$/) {
	$fh = IO::Uncompress::Gunzip->new($file) or die "gunzip failed $GunzipError\n";

}
else {
	open($fh,"<",$file) or die "Error opening $file:$!\n";
	binmode($fh);
}
my $p = STDF::Parser->new( stdf => $fh , exclude_records => 'DTR,GDR' , omit_optional_fields => 0);

my $db = $dbfile;
#my $dsn = "dbi:Pg:dbname=$db;host=localhost";
my $dsn = "dbi:SQLite:dbname=$db";
my $user = '';
my $password = '';

my $dbh = DBI->connect($dsn,$user,$password , 
	{  
	   RaiseError =>  1,
	   AutoCommit => 0
	  }
);
#$dbh->begin_work;

my $insert_stdf_stmt = q{
	insert into stdf (filename,load_time,"user")
	  values ( ?,?,?) 

};

my $mir_stmt = q{
	insert into mir (id,setup_t,start_t,stat_num,mode_cod,rtst_cod,
	prot_cod,burn_tim,cmod_cod,lot_id,part_typ,node_nam,
	tstr_typ,job_nam, job_rev, sblot_id,
	oper_nam, exec_typ,exec_ver, test_cod, tst_temp,
	user_txt, aux_file, pkg_typ , famly_id, date_cod,
	facil_id, floor_id, proc_id, oper_frq, spec_nam,
	spec_ver, flow_id, setup_id, dsgn_rev, eng_id,
	rom_cod, serl_num, supr_nam) VALUES 
} . " ( " .  join (",",( '?' ) x 39  )   . ")" ;

my $sdr_stmt = q {
	insert into sdr(
	id, head_num, site_grp , site_cnt,
	hand_typ, hand_id ,card_typ, card_id , 
	load_typ, load_id ,dib_typ , dib_id  , 
	cabl_typ , cabl_id ,cont_typ, cont_id , 
	lasr_typ,  lasr_id,extr_typ, extr_id) 
	values (
	   ?  , ? , ? , ?, 
	   ?  , ? , ? , ?,
	   ?  , ? , ? , ?,
	   ?  , ? , ? , ?,
	   ?  , ? , ? , ?
	   
	)
};

my $pmr_stmt = q {
	insert into pmr ( 
				stdf_id, pmr_indx, chan_typ, chan_nam, phy_nam, log_nam,
				head_num, site_num )
		values (
				? , ? , ? , ? , ? , ?,
				? , ? 
		)
};

my $pgr_stmt = q {
	insert into pgr (
				stdf_id , grp_indx, grp_nam , pmr_indx )
			values ( ? , ? , ? , ?)
				
};

my $wcr_stmt = q {
	insert into wcr( id ,wafr_siz, die_ht, die_wid,
				     wf_units, wf_flat, center_x,
					 center_y, pos_x, pos_y)
		 values (   ? , ? , ? , ?,
					? , ? , ? ,
					? , ? , ? )
};

my $sdr_sites_stmt = q {
	insert into sdr_sites ( id, head_num, site_grp, site_num) values (
	                         ?,   ?  , ?  , ? )
};

my $sbr_stmt = q {
	insert into sbr (
	  id , head_num , site_num, sbin_num, sbin_cnt, sbin_pf , sbin_nam) 
	  VALUES 
	  ( ? , ? , ? ,? ,? ,? ,?)
};

my $hbr_stmt = q {
	insert into hbr (
		id, head_num, site_num, hbin_num, hbin_cnt , hbin_pf, hbin_nam)
		VALUES 
		(  ? , ? , ? ,? ,? ,? ,?)
	
};

my $pcr_stmt = q {
	insert into pcr (
		id , head_num, site_num,
		part_cnt, rtst_cnt, abrt_cnt,
		good_cnt, func_cnt
	) values 
	(    ?  ,  ?  , ? ,
	     ?  ,  ?  , ? ,
		 ?  ,  ?  )

};

my $mrr_stmt  = q {
	insert into mrr (
		id, finish_t , disp_cod , user_desc, exc_desc) values (
		 ? ,  ? , ? , ? , ? )
		
	
};

my $wir_stmt = q{
	insert into wir (id , head_num,site_grp,start_t,wafer_id)
		values      ( ? ,  ? , ? , ? , ? )
};
my $wrr_stmt = q{
	insert into wrr (id, head_num, site_grp, finish_t, part_cnt ,
	rtst_cnt, abrt_cnt, good_cnt, func_cnt , wafer_id ,
	fabwf_id , frame_id , mask_id , usr_desc,  exc_desc) values 
	( ?, ? , ? ,? ,? ,
	  ?, ? , ? ,? ,? ,
	  ?, ? , ? ,? ,? 
	  )
};


my $prr_stmt = q{
	insert into prr (id,head_num,site_num, part_flg, num_test,
	hard_bin,soft_bin, x_coord, y_coord, test_t, 
	part_id,part_txt, part_fix) 
	VALUES 
	( ?,?,?,?,?,
	  ?,?,?,?,?,
	  ?,?,?)
};

## TEST EXECUTION RECORDS 

my $ptr_stmt = q{
	insert into ptr (part_id, test_flg, parm_flg, result, opt_flag,lo_limit,hi_limit, static_info_id )
	   values       ( ?  , ? ,?, ? , ? , ? ,? , ?)
};

my $mpr_stmt = q {
	insert into mpr (part_id, test_flg, parm_flg, rtn_icnt, rslt_cnt ,
	                 rtn_stat, rtn_rslt , start_in , incr_in, rtn_indx,
					 opt_flag , static_info_id) 
			VALUEs  (  ? , ? , ? , ? ,  ? ,
					   ? , ? , ? , ? ,  ? ,
					   ? , ?
					)
};

my $ftr_stmt = q {
	insert into ftr ( part_id, test_flg, opt_flag, cycl_cnt, rel_vadr ,
	                  rept_cnt, num_fail, xfail_ad, yfail_ad, vect_off ,
					  rtn_icnt, pgm_icnt, rtn_indx, rtn_stat, pgm_indx,
					  pgm_stat, fail_pin, vect_nam, time_set, op_code,
					  test_txt, alarm_id, prog_txt, rslt_txt, patg_num , spin_map, static_info_id)
				VALUES (
					   ? , ? , ? , ? , ? ,
					   ? , ? , ? , ? , ? ,
					   ? , ? , ? , ? , ? ,
					   ? , ? , ? , ? , ? ,
					   ? , ? , ? , ? , ? , ? , ?
				
				)
};

my $ftr_static_stmt = q {
	insert into test_static_info ( stdf_id, test_num , rtn_indx, vect_nam, time_set ,
	                                op_code, test_txt, prog_txt, rslt_txt)
						VALUES    (   ?  , ? , ? , ?, ?,
						               ? , ? ,?  , ?
								)
		
};
my $test_static_stmt = q{
	insert into test_static_info ( stdf_id ,test_num, test_txt, alarm_id, res_scal,
				          llm_scal, hlm_scal, lo_limit, hi_limit, units,
						  c_resfmt, c_llmfmt, c_hlmfmt , lo_spec, hi_spec,
						  rtn_indx, start_in, incr_in, units_in
						  )
			values (       ?, ? , ? , ? , ? ,
						   ? , ? , ? , ? , ?,
						   ? , ? , ? , ? ,? ,
						   ?,?,? , ?
				)
			
};

my $prr_sth = $dbh->prepare($prr_stmt);
my $ptr_sth = $dbh->prepare($ptr_stmt);
my $mpr_sth = $dbh->prepare($mpr_stmt);

my $ftr_static_sth = $dbh->prepare($ftr_static_stmt);
my $ftr_sth        = $dbh->prepare($ftr_stmt);
my $static_sth = $dbh->prepare($test_static_stmt);

my $pmr_sth = $dbh->prepare($pmr_stmt);
my $pgr_sth = $dbh->prepare($pgr_stmt);
my $insert_time = localtime->strftime($SQLITE_TIMESTAMP_FMT);
$dbh->do($insert_stdf_stmt,undef,basename($file),$insert_time,"Data::Loader");
my $stdf_id = $dbh->last_insert_id(undef,undef,'stdf',undef);

my $EPS = 1e-6;

my %count;
my $stream = $p->stream();
$Data::Dumper::Terse = 1;        # don't output names where feasible
$Data::Dumper::Indent = 0;       # turn off all pretty print

my %ptr_info;   # PTR static data part 
my %ptr_data;  # temporary store PTR related to current PRR
my %ptr_default_data;  # store 1st PTR default values
my %mpr_data;
my %mpr_info;
my %mpr_default_data;
my %ftr_data;
my %ftr_default_data;
my %ftr_info;

my $part_count = 0;
## populate STDF name 
##                        STDF   
###                        |
###                MIR 
while(my $r = $stream->()) {
	#my $str = join "|",@$r;
	#print "$str\n";
	#print Dumper($r),"\n";
	$count{$r->[0]}++;
	my $t = $r->[0];
	
	if($t eq "MIR") {
		my @mir = @$r;
		$mir[0] = $stdf_id;
		$mir[1] = localtime($mir[1])->strftime($SQLITE_TIMESTAMP_FMT);
		$mir[2] = localtime($mir[2])->strftime($SQLITE_TIMESTAMP_FMT);
		$mir[38] = undef;
		$dbh->do($mir_stmt,undef,@mir);
	}
	elsif($t eq "PTR") {
		my ($head,$site) = @$r[2,3];
		# we do insertion of test execution data at part level (PRR)
		#print "stor $head $site PTR\n";
		push @{$ptr_data{$head,$site}},$r;
	}
	elsif($t eq "FTR") {
		my ($head,$site) = @$r[2,3];
		push @{$ftr_data{$head,$site}},$r;
	}
	elsif($t eq "MPR" ) {
		my ($head,$site) = @$r[2,3];
		push @{$mpr_data{ $head,$site}},$r;
	}
	elsif($t eq "PMR") {
	#stdf_id, pmr_indx, chan_typ, chan_nam, phy_nam, log_nam,
	#			head_num, site_num )
		my @pmr_fields = @$r;
		shift(@pmr_fields); # remove rec name 
		$pmr_sth->execute($stdf_id,@pmr_fields);
	}
	elsif($t eq "PGR") {
		my @pgr_fields = @$r;
		shift(@pgr_fields);
		$pgr_sth->execute($stdf_id,@pgr_fields[0,1,3]);
	}
	elsif($t eq "WCR" ){
		my @wcr = @$r;
		$wcr[0] = $stdf_id;
		
		$wcr[9] = undef if(@wcr < 10);
		$dbh->do($wcr_stmt,undef,@wcr);
	}
	elsif($t eq "SDR") {
	#20|1|255|ARRAY(0x15b1560)|OPUS042||BCM4388REVB-8X-21064-MOB-20-0042-SN:000201-02-22||||||||TTS|||||
		
		my @sdr = @$r;
		$sdr[0] = $stdf_id;
		print Dumper(\@sdr),"\n";
		my ($head,$grp) = @sdr[1,2];
		my $sites_ref = $sdr[3];
		$sdr[3] = scalar(@$sites_ref);
		$sdr[19] = undef if(@sdr < 20);
		$dbh->do($sdr_stmt,undef,@sdr);
		#id, head_num, group, site_num
		
		for my $site_num(@$sites_ref) {
			print "insert individual site num for $site_num\n";
			$dbh->do($sdr_sites_stmt,undef,$stdf_id,
			$head,$grp,$site_num);
		}
	}
	elsif($t eq "WIR") {
		my @wir = @$r;
		$wir[0] = $stdf_id;
		$wir[3] = localtime($wir[3])->strftime($SQLITE_TIMESTAMP_FMT);
		$dbh->do($wir_stmt,undef,@wir);
	}
	elsif($t eq "WRR" ) {
		my @wrr = @$r;
		$wrr[0] = $stdf_id;
		$wrr[3] = localtime($wrr[3])->strftime($SQLITE_TIMESTAMP_FMT);
		$wrr[14] = undef if(@wrr < 15);
		$dbh->do($wrr_stmt,undef,@wrr);
	}
	elsif($t eq "HBR" || $t eq "SBR") {
		my @hbr =@$r;
		$hbr[0] = $stdf_id;
		$hbr[6] = undef if(@hbr < 7);
		my $stmt = $t eq "HBR" ? $hbr_stmt : $sbr_stmt;
		$dbh->do($stmt,undef,@hbr);
	}
	elsif($t eq "PCR") {
		my @pcr = @$r;
		$pcr[0] = $stdf_id;
		$pcr[7] = undef if(@pcr<8);
		$dbh->do($pcr_stmt,undef,@pcr);
	}
	elsif($t eq "MRR") {
		my @mrr = @$r;
		$mrr[0] = $stdf_id;
		$mrr[1] = localtime($mrr[1])->strftime($SQLITE_TIMESTAMP_FMT);
		$mrr[4] = undef if(@mrr < 5);
		$dbh->do($mrr_stmt,undef,@mrr);
	}
	elsif($t eq "PIR") {
		## use PIR to clear test execution data 
		my ($h,$s) = @$r[1,2];
		delete $ptr_data{$h,$s};
		delete $mpr_data{$h,$s};
		delete $ftr_data{$h,$s};
	}
	elsif($t eq "PRR") {
		$r->[0] = $stdf_id;
		if(defined($r->[7]) && $r->[7] == -32768)  { $r->[7] =undef; }
		if(defined($r->[8]) && $r->[8] == -32768)  { $r->[8] = undef; }
		if(defined($r->[6]) && $r->[6] == 65535)   { $r->[6] = undef; }
		$r->[12] = undef if(@$r < 13);
		$prr_sth->execute(@$r);
		$part_count++;
		my $last_prr_id = $dbh->last_insert_id(undef,undef,'prr',undef) ;
		## insert the part info first 
		## then test excuctions like PTR,FTR,MPR for that part
		
		if(!defined($last_prr_id)) { die "PRR insert error:\n"; }
		my ($head,$site) = @$r[1,2];
		my $ptr_ref = $ptr_data{$head,$site};
		my $mpr_ref = $mpr_data{$head,$site};
		my $ftr_ref = $ftr_data{$head,$site};
		#print "PTR for $head,$site\n";
		my @ptrs;
		my @mprs;
		my @ftrs;
		if(defined($ptr_ref)) {
			@ptrs = @$ptr_ref;
		}
		if(defined($mpr_ref)) {
			@mprs = @$mpr_ref;
		}
		if(defined($ftr_ref)) {
			@ftrs = @$ftr_ref;
		}
		if($part_count %100 ==0) { 
		print "insert PTR for this Part $part_count $head,$site ", scalar(@ptrs),"\n";
		print "insert MPR for this part , " ,scalar(@mprs),"\n";
		print "insert FTR for this part , ", scalar(@ftrs),"\n";
		}
		### test execution records insertion 
		## 
		## normalise these records optional semi-static in separate table to reduce space 
		## repeated strings like test_text are stored only once 
		
		## PTR main table 
		#   ptr_id   - primary AI for this PTR 
		#   prr_id   - link to Part, belong to this PRR
		#   test_flg
		#   parm_flg
		#   result    - NULL
		#   test_txt - NULL    symbol data type
		#   alarm_id     - NULL   symbol data type
		#   opt_flg   - NULL
		#   ptr_opt_id - optional semi-static ID
		
		# ptr_opt   hold semi static information for PTR
		#  ptr_info_id 
		#  stdf_id 
		#  test_num
		#  res_scal 
		#  low_spec
		#   hi_spec 
		
		for my $ptr(@ptrs) {
			my @ptr_field = @$ptr;  ## treat as const, do not remove/add elements
			
			## insert logic 
			## for 1st PTR record for test_num, with complete information (info after opt_flag) 
			## populate to ptr_info
			## keep table of PTR_INFO [test_num]
			my $test_num = $ptr_field[1];
			#print "INSERT "; print Dumper(\@ptr_field),"\n";
			my $result = $ptr_field[6];
			my $test_flg = $ptr_field[4];
			my $parm_flg = $ptr_field[5];
			my ($dynamic_low_limit,$dynamic_hi_limit); # for dynamic test limits
			#TEST_FLG bit 0 = 0 no alarm
			# bit 1 = 0 value in result field is valid
			# bit 2 = 0 test result is reliable
			# bit 3 = 0 no timeout
			# bit 4 = 0 test was executed
			# bit 5 = 0 no abort
			# PARM_FLG bit 0 = 0 no scale error
			# bit 1 = 0 no drift error
			# bit 2 = 0 no oscillation
			if( ($test_flg & 0x3E) == 0 && 
				($parm_flg & 0x06) == 0 ) {
				
				# result is valid 
			} else {
				$result = undef;
			}
			my $test_txt = $ptr_field[7];

			if(@ptr_field > 10) {
			## got optional semi static data
			## either 1st default data or this test exec contain different values from default 
			
				my $opt_flag = $ptr_field[9];
				my $res_scal = ($opt_flag & 0x01 ) ? undef : $ptr_field[10];
				my $lo_spec  = ($opt_flag & 0x04 ) ? undef : $ptr_field[19];
				my $hi_spec  = ($opt_flag & 0x08 ) ? undef : $ptr_field[20];
				my ($lo_limit,$llm_scal,$hi_limit,$hlm_scal); # default undef
				if( ($opt_flag & 0x50) == 0) { # bit 4,6 not set 
					$lo_limit = $ptr_field[13];
					$llm_scal = $ptr_field[11];
				}
				if( ($opt_flag & 0xA0) == 0) {  # bit 5,7 not set
					$hi_limit = $ptr_field[14];
					$hlm_scal = $ptr_field[12];
				}
				@ptr_field[10,11,12,13,14,19,20]= ($res_scal,$llm_scal,$hlm_scal,
				                                    $lo_limit,$hi_limit,$lo_spec,$hi_spec);
				my $insert_static_data = 0;
				my $default_exists = exists($ptr_default_data{$test_num,$test_txt}) || exists($ptr_default_data{$test_num});

				unless( $default_exists )  {
					## wanna make sure all limits are set properly here 
					$insert_static_data = 1;
					my $def_ptr_fields = [@ptr_field];
					print "First time for $test_num, $test_txt \n";
					print Dumper(\@ptr_field),"\n";
					$ptr_default_data{$test_num,$test_txt} = $def_ptr_fields;
					$ptr_default_data{$test_num} = $def_ptr_fields;
					
				} else {
					my $defaults = exists($ptr_default_data{$test_num,$test_txt}) ? $ptr_default_data{$test_num,$test_txt} : 
								$ptr_default_data{$test_num};
					my ($d_lo_limit,$d_lo_scal,$d_hi_limit,$d_hi_scal) = @$defaults[13,11,14,12];
					my $same_lo_limit = 0;
					my $same_hi_limit = 0; 
					if(defined($lo_limit) && defined($llm_scal)
						&& abs($lo_limit - $d_lo_limit) < $EPS && $llm_scal == $d_lo_scal
					) {
						$same_lo_limit = 1;
					}
					if(defined($hi_limit) && defined($hlm_scal) &&
						abs($hi_limit - $d_hi_limit) < $EPS && $hlm_scal == $d_hi_scal ) {
						$same_hi_limit = 1;
					}
					$insert_static_data = !( $same_lo_limit && $same_hi_limit);
				}
				if($insert_static_data) {
					my @static_fields = ($stdf_id,$test_num,@ptr_field[7,8,10..20 ],undef,undef,undef,undef);
					$static_sth->execute(@static_fields);
					my $inserted_id = $dbh->last_insert_id(undef,undef,'test_static_info',undef);
					 $ptr_info{$test_num,$test_txt} = $inserted_id;
					 $ptr_info{$test_num} = $inserted_id;
					print "insert default for $test_num|$test_txt|$inserted_id \n";
					$dynamic_low_limit = $lo_limit;
					$dynamic_hi_limit  = $hi_limit;

				}
			}
			$ptr_sth->execute($last_prr_id,@ptr_field[4,5,6,9],$dynamic_low_limit,$dynamic_hi_limit,
			exists($ptr_info{$test_num,$test_txt}) ? $ptr_info{$test_num,$test_txt}: $ptr_info{$test_num});
			
		}
		
		for my $mpr(@mprs) {
			my @mpr_field = @$mpr;
			my $test_num = $mpr_field[1];
			my $test_txt = $mpr_field[10];
			my $rtn_result = $mpr_field[9]; #could be empty ; type array ref
			my $encoded_array ;
			if(defined($rtn_result) && @$rtn_result) {
				$encoded_array = encode_base64( pack("f<*",@$rtn_result) );
				#$encoded_array =  pack("f<*",@$rtn_result) ;
			}
			my $rtn_indx;

			if(@mpr_field > 13) {
			#	print "MPR got optional \n";
				my $opt_flag = $mpr_field[12];
				my $res_scal = ($opt_flag & 0x01 ) ? undef : $mpr_field[13];
				my $lo_spec  = ($opt_flag & 0x04 ) ? undef : $mpr_field[26];
				my $hi_spec  = ($opt_flag & 0x08 ) ? undef : $mpr_field[27];
				my ($lo_limit,$llm_scal,$hi_limit,$hlm_scal); # default undef
				if( ($opt_flag & 0x50) == 0) { # bit 4,6 not set 
					$lo_limit = $mpr_field[16];
					$llm_scal = $mpr_field[14];
				}
				if( ($opt_flag & 0xA0) == 0) {  # bit 5,7 not set
					$hi_limit = $mpr_field[17];
					$hlm_scal = $mpr_field[15];
				}
				my $insert_static = 0;
				my $default_exists = exists($mpr_info{$test_num,$test_txt}) && exists($mpr_info{$test_num});
				unless( $default_exists) {
				# first default MPR 
					$insert_static = 1;
					my $def_mpr = [@mpr_field];
					$mpr_default_data{$test_num,$test_txt} = $def_mpr;
					$mpr_default_data{$test_num} = $def_mpr;
				}
				else {
					# compare this with 1st MPR values 
					## compare ON 
					#  RES_SCAL, LLM_SCAL, HLM_SCAL 
					#  LO_LIMIT, HI_LIMIT , START_IN,INCR_IN, UNITS 

					my $mpr_default;
					if( exists($mpr_default_data{$test_num,$test_txt}) ) {
						$mpr_default = $mpr_default_data{$test_num,$test_txt};
					}
					elsif(exists($mpr_default_data{$test_num})) {
						$mpr_default = $mpr_default_data{$test_num};
					}
					else {

						# what could be the reason?
						goto done_mpr_default;
					}
					
					my ($d_res_scal,$d_llm_scal,$d_hlm_scal) = @$mpr_default[13,14,15];
					my ($d_lo_limit,$d_hi_limit) = @$mpr_default[16,17];
					#print "NOT FIRST MPR!\n";
					#my ($d_units,$d_units_in,$d_resfmt,$d_llmfmt,$d_hlmfmt) = @$mpr_result[21,22,23,24,25];

					if(defined($res_scal) && $res_scal != $d_res_scal) { $insert_static = 1; }
					if(defined($llm_scal) && $llm_scal != $d_llm_scal) { $insert_static = 1; }
					if(defined($hlm_scal) && $hlm_scal != $d_hlm_scal) { $insert_static = 1;  }
					if(defined($hi_limit) && abs( $hi_limit - $d_hi_limit) > $EPS ) { $insert_static = 1; }
					if(defined($lo_limit) && abs( $lo_limit - $d_lo_limit) > $EPS) { $insert_static = 1; }
					for(21,22,23,24,25) {
						if( defined($mpr_default->[$_]) && defined($mpr_field[$_]) && $mpr_default->[$_] ne $mpr_field[$_]) {
							$insert_static = 1;
							last;
						}
					}
				}
				done_mpr_default:
				if($insert_static) {
					my $indx_arr = $mpr_field[20];
					if(defined($indx_arr) && @$indx_arr) {
					$rtn_indx = encode_base64( pack("f<*",@$indx_arr));
					#$rtn_indx =  pack("f<*",@$indx_arr);
					}
					my @static_fields = ($stdf_id,$test_num,$test_txt,
					   @mpr_field[11,13,14,15,16,17,21,23,24,25,26,27],$rtn_indx, 
					     @mpr_field[18,19,22]);
					$static_sth->execute(@static_fields);
					my $inserted_id = $dbh->last_insert_id(undef,undef,'test_static_info',undef);
					$mpr_info{$test_num,$test_txt} = $inserted_id;
					$mpr_info{$test_num} = $inserted_id;
				}
			}
			$mpr_sth->execute($last_prr_id,@mpr_field[4,5,6,7,8],$encoded_array,@mpr_field[18,19],$rtn_indx,$mpr_field[12],
				exists($mpr_info{$test_num,$test_txt}) ? $mpr_info{$test_num,$test_txt} : $mpr_info{$test_num}   );
		}
		
		for my $ftr(@ftrs) {
		# for FTR 
		## only insert 
		## test_flg and opt_flag 
		## the rest of field if invalid, insert NULL
		## if 1st for this tnum+vect_nam,time_set,op_code,test_txt
			my @ftr_field = @$ftr;
			my $tnum = $ftr_field[1];
			my $ftr_key = join "",
			map { defined($_) ? $_:'' } @ftr_field[1,20,21,22,23,25,26];
			
			my $opt_flag = $ftr_field[5];
			if(defined($opt_flag)){
				$ftr_field[6] = undef if($opt_flag & 0x01);  # CYCL_CNT invalid if OPT_FLAG bit 0 = 1
				$ftr_field[7] = undef if($opt_flag & 0x02);  #REL_VADR invalid if OPT_FLAG bit 1 = 1
				$ftr_field[8] = undef if($opt_flag & 0x04);  # REPT_CNT invalid if OPT_FLAG bit 2 = 1
				$ftr_field[9] = undef if($opt_flag & 0x08);  # NUM_FAIL invalid if OPT_FLAG bit 3 = 1
				$ftr_field[10]= undef if($opt_flag & 0x10);  # XFAIL_AD invalid if OPT_FLAG bit 4 = 1
				$ftr_field[11]= undef if($opt_flag & 0x10);  # YFAIL_AD invalid if OPT_FLAG bit 4 = 1
				$ftr_field[12]= undef if($opt_flag & 0x20);  # VECT_OFF invalid if OPT_FLAG bit 5 = 1
				
				if(! defined($ftr_field[13]) || $ftr_field[13] == 0) {
					$ftr_field[15] = $ftr_field[16] = undef;
				}
				else {
					## gotta pack them back 
					my $rtn_indx = $ftr_field[15];
					
					$ftr_field[15] = encode_base64( pack("f<*",@$rtn_indx));
					#$ftr_field[15] =  pack("f<*",@$rtn_indx);
					## rtn_stat already hex string 
					
				}
				if(! defined($ftr_field[14]) || $ftr_field[14] == 0) {
					$ftr_field[17] = $ftr_field[18] = undef;
				}
				else {
					my $pgm_indx = $ftr_field[17];
					$ftr_field[17] = encode_base64( pack("f<*",@$pgm_indx));
					#$ftr_field[17] =  pack("f<*",@$pgm_indx);
				}
				
				if(defined($ftr_field[19])) {
					my $fail_pin = $ftr_field[19];
					$ftr_field[19] = encode_base64( pack("C*",@$fail_pin));
					#$ftr_field[19] =  pack("C*",@$fail_pin);

				}
				
			}
			
			unless( exists($ftr_info{$ftr_key})) {
			
			#	insert into test_static_info ( stdf_id, test_num ,vect_nam, time_set ,
	         #                       op_code, test_txt, prog_txt, rslt_txt)

				$ftr_static_sth->execute($stdf_id, $tnum, 
				@ftr_field[15,20,21,22,23,25,26]);
				my $inserted_id = $dbh->last_insert_id(undef,undef,'test_static_info',undef);
				$ftr_info{$ftr_key} = $inserted_id;

			}else {
				## null it out all keys 
				for(15,20,21,22,23,25,26) {
					$ftr_field[$_] = undef;
				}
			}
			
			#	insert into ftr ( part_id, test_flg, opt_flag, cycl_cnt, rel_vadr ,
	        #          rept_cnt, num_fail, xfail_d, yfail_ad, vect_off ,
			#		  rtn_icnt, pgm_icnt, rtn_indx, rtn_stat, pgm_indx,
			#		  pgm_stat, fail_pin, vect_nam, time_set, op_code,
			#		  test_txt, alarm_id, prog_txt, rslt_txt, patg_num , spin_map)
			if(@ftr_field < 28) { $ftr_field[28] = undef;}
			if( defined($ftr_field[28])) {
				$ftr_field[28] = undef;
			}
			$ftr_sth->execute($last_prr_id, @ftr_field[4,5,6..28], $ftr_info{$ftr_key});
			
		}
	}
		
}

$dbh->commit;
