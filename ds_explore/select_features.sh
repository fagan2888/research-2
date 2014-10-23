
time hive -e "
ADD FILE streamer.py;

DROP TABLE field.pfcode_serials_changes;

CREATE EXTERNAL TABLE field.pfcode_serials_changes (
    enddate string,
    pfcode_grp string,
    bootstrap_sample int,
    pfcode string,
    hddsn string,
    slidersn string,
    mtype string,
    pfsubcode string
)
LOCATION '/user/field/field.pfcode_serials_changes'
;

INSERT OVERWRITE TABLE field.pfcode_serials_changes
SELECT TRANSFORM (enddate, pfcode, hddsn, slidersn, mtype, pfsubcode)
USING 'python streamer.py 10 4301 0.00140218547529 4C56 0.0273667923798 C40F 0.00024175611643 6120 0.000435161009574 2521 0.00024175611643 2522 0.0024175611643 6SCX 0.00024175611643 2528 0.000483512232859 FTBL 0.000821970795861 9BAT 0.00933178609419 CRHL 0.00169229281501 CRSF 0.000338458563002 CRHH 0.00024175611643 UAR2 0.0183734648487 CRSQ 0.00323953196016 6SER 0.00357799052316 CAEF 0.00251426361087 CSPI 0.00169229281501 CTSQ 0.00140218547529 CEMT 0.000628565902717 CEMS 0.00575379557103 CFTA 0.000870322019147 4CIK 0.000290107339716 2251 0.00024175611643 CRMP 0.000531863456145 CRTL 0.000338458563002 COLR 0.00106372691229 9991 0.00024175611643 4201 0.00270766850401 989A 0.00304612706701 4801 0.00638236147375 4802 0.00193404893144 4803 0.00290107339716 CRFG 0.00996035199691 6SAT 0.000676917126003 4809 0.00328788318344 6CAT 0.000435161009574 CB01 0.000290107339716 2263 0.000386809786288 6D2C 0.00401315153273 6D2M 0.000338458563002 6PRD 0.000773619572575 6PRE 0.00024175611643 480A 0.00130548302872 3262 0.000531863456145 3154 0.001015375689 CAHD 0.00145053669858 6ASX 0.00116042935886 CAHB 0.00169229281501 C710 0.00464171743545 4101 0.00232085871773 611B 0.000676917126003 CMCL 0.000676917126003 CMCO 0.00024175611643 2207 0.000918673242433 CTFF 0.000386809786288 6ATF 0.00270766850401 CMCU 0.000531863456145 CFAL 0.000386809786288 4LO4 0.00140218547529 CRWF 0.00140218547529 220A 0.000773619572575 4445 0.001015375689 3288 0.00212745382458 4606 0.00367469296973 FWTO 0.000676917126003 4601 0.000290107339716 2211 0.000290107339716 CMBG 0.000290107339716 4C11 0.000435161009574 4C12 0.00106372691229 Other 0.00938013731747 6FWA 0.000386809786288 4C16 0.000676917126003 CBMS 0.000338458563002 3223 0.000290107339716 CADF 0.000435161009574 CMTW 0.000338458563002 CTDT 0.000483512232859 CAMP 0.000386809786288 CWHL 0.000435161009574 989E 0.001015375689 6PAD 0.000483512232859 989B 0.0065274151436 989C 0.001015375689 CRFI 0.00304612706701 CCCF 0.00130548302872 4502 0.000435161009574 9SHD 0.000386809786288 CERD 0.000338458563002 6OVT 0.000821970795861 6TAH 0.000386809786288 9804 0.000918673242433 9803 0.000821970795861 9802 0.000725268349289 9801 0.00154723914515 3222 0.000338458563002 CDAH 0.00140218547529 0150 0.000338458563002 4001 0.00319118073687 6TAS 0.000870322019147'
AS (enddate, pfcode_grp, bootstrap_sample, pfcode, hddsn, slidersn, mtype, pfsubcode)
FROM (
    SELECT *
    FROM venusq.association
    WHERE
        year=2014 AND
        month=07 AND
        --SUBSTR(enddate,0,8)=date AND
        slidersn IS NOT NULL) asso
"



time hive -f sldr.bcslider.hql > sldr.bcslider_w_nonnull_counts.csv


time hive -f sldr.decodet_deduped.hql > sldr.decodet_deduped_w_nonnull_counts.csv


time hive -f sldr.decoquasi.hql > sldr.decoquasi_w_nonnull_counts.csv


time hive -f sldr.etchdepth.hql > sldr.etchdepth_w_nonnull_counts.csv


time hive -f sldr.flatness_deduped.hql > sldr.flatness_deduped_w_nonnull_counts.csv


time hive -f sldr.jade_deduped.hql > sldr.jade_deduped_w_nonnull_counts.csv


time hive -f sldr.lapfinalsubphase_deduped.hql > sldr.lapfinalsubphase_deduped_w_nonnull_counts.csv


time hive -f sldr.lapsubphaseslider.hql > sldr.lapsubphaseslider_w_nonnull_counts.csv


time hive -f sldr.sliderbin.hql > sldr.sliderbin_w_nonnull_counts.csv


time hive -f sldr.sliderbinhist.hql > sldr.sliderbinhist_w_nonnull_counts.csv


time hive -f sldr.sliderdefecthist.hql > sldr.sliderdefecthist_w_nonnull_counts.csv


time hive -f sldr.sliderdefectjrnl.hql > sldr.sliderdefectjrnl_w_nonnull_counts.csv


time hive -f sldr.sliderhist_deduped.hql > sldr.sliderhist_deduped_w_nonnull_counts.csv


time hive -f sldr.sliderquasi.hql > sldr.sliderquasi_w_nonnull_counts.csv


time hive -f usercontrib.nakagawa_stream_aug18.hql > usercontrib.nakagawa_stream_aug18_w_nonnull_counts.csv