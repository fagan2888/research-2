#
# Params
#

PRODUCT="cbf"

YEAR="2014"
MONTH="07"
DAY="01"
ENDDATE="$YEAR$MONTH$DAY"

ASSOCIATION_TABLE="venusq.association"

DBHOST="bdp-dev-mkii.caadsjvtzcmw.ap-northeast-1.rds.amazonaws.com"
DBNAME="datascience"
DBUSER="datascientist"
DBPASSWORD="DataGuru14"

ERROR_COUNT_FILE="error_count.csv"

TARGET_SLIDERSN_TABLE="field.pfcode_serials_changes"

NUM_BOOTSTRAP_SAMPLES="10"
NUM_TOP_ERRORS="100"
NUM_TOP_FEATURES="20"

#
# Calculate num failing HDDs per pfcode/day
# * Assoc table will have same hdd/date appear multiple times,
#   once per slider in the HDD.
#

# Took 1 hour to run, July 7th
time hive -e "
SELECT the_date, pfcode, COUNT(*)
FROM (
    SELECT DISTINCT
        SUBSTR(enddate,0,8) as the_date,
        hddsn,
        pfcode
    FROM $ASSOCIATION_TABLE
    WHERE
        product='$PRODUCT' AND
        year=$YEAR AND
        month=$MONTH
        AND (SUBSTR(enddate,0,8)='20140714' or SUBSTR(enddate,0,8)='20140715' or 
            SUBSTR(enddate,0,8)='20140716' or SUBSTR(enddate,0,8)='20140717' or SUBSTR(enddate,0,8)='20140718')
        --AND SUBSTR(enddate,0,8)='$ENDDATE'
    ) assoc
GROUP BY the_date, pfcode
" > $ERROR_COUNT_FILE




#
# Find slidersns to study.
# Join slider parametrics and Nakagawa stream against them.
# Calculate top features
#

python write_feature_selector.py \
    --error_count_file=$ERROR_COUNT_FILE \
    --year=$YEAR \
    --month=$MONTH \
    --num_bootstrap_samples=$NUM_BOOTSTRAP_SAMPLES \
    --num_top_errors=$NUM_TOP_ERRORS \
    --num_top_features=$NUM_TOP_FEATURES \
    --target_slidersn_table=$TARGET_SLIDERSN_TABLE \
    --association_table=$ASSOCIATION_TABLE

sh select_features.sh


#
# Reformat Hive output
#

python format_features.py \
    --date=$ENDDATE \
    --error_count_file=$ERROR_COUNT_FILE \
    --year=$YEAR \
    --month=$MONTH \
    --num_bootstrap_samples=$NUM_BOOTSTRAP_SAMPLES \
    --num_top_errors=$NUM_TOP_ERRORS \
    --num_top_features=$NUM_TOP_FEATURES \
    --target_slidersn_table=$TARGET_SLIDERSN_TABLE \
    --association_table=$ASSOCIATION_TABLE



#
# Load top features onto the RDS
#

mysqlimport --local \
-u $DBUSER \
-h $DBHOST \
--password=$DBPASSWORD \
--fields-terminated-by='\t' \
$DBNAME top_detractors.csv

mysqlimport --local \
-u $DBUSER \
-h $DBHOST \
--password=$DBPASSWORD \
--fields-terminated-by='\t' \
$DBNAME top_features.csv

