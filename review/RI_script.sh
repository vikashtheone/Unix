RI Script for multiple tables using parameterization

PARM_FILE=$1
PRNT_TBL_NM=$2
CHILD_TBL_NM=$3

ST_TIME=`date +%s`

. $CODE/scripts/$PARM_FILE.parm

if [ -f $PMDIR/SrcFiles/Sample_ri_chck_$2_$3.txt ];then
rm -f $PMDIR/SrcFiles/Sample_ri_chck_$2_$3.txt
fi

bteq <<EOF

.SET WIDTH 150;

/*********************************************************************************************
put BTEQ in Transaction mode
*********************************************************************************************/

.SET SESSION TRANSACTION BTET;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

/**************************************************************************************************
Parms are set in the POST_LOAD_RI_CHK_SGMT.parm file in the .sh file that calls this .ctl file.
Extract username and password and logon to database.
**************************************************************************************************/

.run file $LOGON/$LOGON_ID;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

SELECT SESSION;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

SET QUERY_BAND = 'ApplicationName=$0;Frequency=weekly;' FOR SESSION;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

/**************************************************************************************************
Set default database based on parmeter in parm file if needs to be different than the logon ID's 
default database.
Set default database derived from parameter file
**************************************************************************************************/

DATABASE $ETL_VIEWS_DB;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/


.EXPORT DATA FILE=$PMDIR/SrcFiles/Sample_ri_chck_$2_$3.txt

.SET RECORDMODE OFF;

.SET WIDTH 2000;

SELECT TRIM(PRNT_TBL_NM) ||'|'||TRIM(CHILD_TBL_NM)||'|'||TRIM(CAST(PARENT_CLMN_KEY AS VARCHAR(2000))) ||'|'||
TRIM(CAST(CHILD_CLMN_KEY AS VARCHAR(2000)))||'|' from $ETL_VIEWS_DB.RI_PK_XREF where PRNT_TBL_NM= '${PRNT_TBL_NM}' AND CHILD_TBL_NM='${CHILD_TBL_NM}' ;


/***************** Error Handling ********************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/***************** Error Handling ********************************/

.EXPORT RESET

.LABEL ERRORS
.QUIT ERRORCODE
EOF

JOINCNDTN1=""
JOINCNDTN=""
CHLD_VAR=""

i=$(head -1 $PMDIR/SrcFiles/Sample_ri_chck_$2_$3.txt)

echo $i
PRNT_TBL_NM=`echo $i | awk -F"|" '{ print $1 }'`
CHLD_TBL_NM=`echo $i | awk -F"|" '{ print $2 }'`
PRNT_CLMN_NM=`echo $i | awk -F"|" '{ print $3 }'`
echo "PRNT_CLMN_NM="${PRNT_CLMN_NM}
CHLD_CLMN_NM=`echo $i | awk -F"|" '{ print $4 }'`
echo "CHLD_CLMN_NM="${CHLD_CLMN_NM}


POST_RI_EDW_LLK_VT=`echo CREATE MULTISET VOLATILE TABLE POST_RI_EDW_LOAD_LOG_VT as \(SELECT LOAD_LOG_KEY FROM  table_1 where  SUBJ_AREA_NM=\'${SUBJ_AREA_NM}\' AND WORK_FLOW_NM=\'${WORK_FLOW_NM2}\' AND PBLSH_IND=\'N\'\) with data primary index \(LOAD_LOG_KEY\) on commit preserve rows\;`

EDW_LLK_VT=`echo CREATE MULTISET VOLATILE TABLE EDW_LOAD_LOG_VT as \(SELECT LOAD_LOG_KEY FROM  table_1 where  SUBJ_AREA_NM=\'${SUBJ_AREA_NM}\' AND WORK_FLOW_NM=\'${WORK_FLOW_NM}\' qualify row_number\(\)over \(partition by SUBJ_AREA_NM, WORK_FLOW_NM order by pblsh_dtm desc\) = 1\) with data primary index \(LOAD_LOG_KEY\) on commit preserve rows\;`

PRNT_TBL_VT=`echo CREATE MULTISET VOLATILE TABLE ${PRNT_TBL_NM}_VT as \(SELECT ${PRNT_CLMN_NM} from ${PRNT_TBL_NM} WHERE RCRD_STTS_CD="'ACT'" GROUP BY ${PRNT_CLMN_NM}\) with data primary index \(${PRNT_CLMN_NM}\) on commit preserve rows\;` 
CHLD_TBL_VT=`echo CREATE MULTISET VOLATILE TABLE ${CHLD_TBL_NM}_VT as \(SELECT ${CHLD_CLMN_NM},RCRD_STTS_CD,X.LOAD_LOG_KEY,UPDTD_LOAD_LOG_KEY FROM \(SELECT ${CHLD_CLMN_NM},LOAD_LOG_KEY,UPDTD_LOAD_LOG_KEY,RCRD_STTS_CD from ${CHLD_TBL_NM} WHERE RCRD_STTS_CD="'ACT'" GROUP BY \( ${CHLD_CLMN_NM},RCRD_STTS_CD,LOAD_LOG_KEY,UPDTD_LOAD_LOG_KEY\)\) X CROSS JOIN EDW_LOAD_LOG_VT\) with data primary index \(${CHLD_CLMN_NM}\) on commit preserve rows\;` 

NoColPK=`echo $PRNT_CLMN_NM|awk -F"," '{print NF}'` 
echo "NoColPK="$NoColPK

echo " POST_RI_EDW_LLK_VT"=$POST_RI_EDW_LLK_VT
echo "\n "
echo " EDW_LLK_VT"=$EDW_LLK_VT
echo "\n "
echo " PRNT_TBL_VT"=$PRNT_TBL_VT
echo "\n "
echo " CHLD_TBL_VT"=$CHLD_TBL_VT
echo "\n "

echo "PRNT_CLMN_NM"=$PRNT_CLMN_NM
echo "CHLD_CLMN_NM"=$CHLD_CLMN_NM
PRNTColNM=""
CHLDColNM=""
JOINCNDTN1=""
CHLD_VAR=""
PRNT_VAR=""
j=1
while [ $j -le $NoColPK ]
do
PRNTColNM=`echo ${PRNT_CLMN_NM}|cut -d"," -f$j`
CHLDColNM=`echo ${CHLD_CLMN_NM}|cut -d"," -f$j`
if [ $j -eq $NoColPK ];then
###############JOINCNDTN=`echo "LZ."${LZColNM}"=EDW."${EDWColNM}`#######################
JOINCNDTN1=`echo ${JOINCNDTN1} ${CHLD_TBL_NM}"."${CHLDColNM}"="${PRNT_TBL_NM}"."${PRNTColNM}`
JOINCNDTN2=`echo "WHERE "${PRNT_TBL_NM}"."${PRNTColNM} "IS NULL"`
JOINCNDTN3=`echo ${JOINCNDTN1} ${JOINCNDTN2}`
CHLD_VAR=`echo ${CHLD_VAR} ${CHLD_TBL_NM}"."${CHLDColNM}`
PRNT_VAR=`echo ${PRNT_VAR} ${CHLD_TBL_NM}"."${CHLDColNM}`
else
#########JOINCNDTN=`echo ${JOINCNDTN}" AND LZ."${LZColNM}"=EDW."${EDWColNM}`#############
JOINCNDTN1=`echo ${JOINCNDTN1} ${CHLD_TBL_NM}"."${CHLDColNM}"="${PRNT_TBL_NM}"."${PRNTColNM} " AND "`
CHLD_VAR=`echo ${CHLD_VAR} ${CHLD_TBL_NM}"."${CHLDColNM}","`

fi

j=$((j+1))
done

COLSTATS_PRNT=`echo COLLECT STATS ON ${PRNT_TBL_NM}_VT INDEX \(${PRNT_CLMN_NM}\)\;`
echo " COLSTATS_PRNT="$COLSTATS_PRNT
echo "\n "

COLSTATS_CHLD=`echo COLLECT STATS ON ${CHLD_TBL_NM}_VT INDEX \(${CHLD_CLMN_NM}\)\;`

echo " COLSTATS_CHLD="$COLSTATS_CHLD
echo "\n "

echo "Join Condition created: "$JOINCNDTN1
echo "\n "

echo "Join Condition 2: "$JOINCNDTN3
echo "\n "
echo "CHLD_VAR: "$CHLD_VAR
echo "\n "

FINAL_TBL_VT=`echo CREATE MULTISET VOLATILE TABLE ${PRNT_TBL_NM}_UPD_VT as \(SELECT distinct $CHLD_VAR,RCRD_STTS_CD from ${CHLD_TBL_NM}_VT ${CHLD_TBL_NM} LEFT JOIN ${PRNT_TBL_NM}_VT ${PRNT_TBL_NM} on ${JOINCNDTN3}\) with data primary index \($CHLD_CLMN_NM\) on commit preserve rows\;`
echo "FINAL_TBL_VT= "$FINAL_TBL_VT
echo "\n "

COLSTATS_FNL=`echo COLLECT STATS ON ${PRNT_TBL_NM}_UPD_VT INDEX \(${CHLD_CLMN_NM}\)\;`

echo " COLSTATS_FNL="$COLSTATS_FNL
echo "\n " 

#UPDATE_STMT=`echo UPDATE ${PRNT_TBL_NM} FROM ${PRNT_TBL_NM}_UPD_VT ${CHLD_TBL_NM} SET RCRD_STTS_CD = "'DEL'" WHERE ${JOINCNDTN1} AND RCRD_STTS_CD ="'ACT'"\;`
#echo "UPDATE_STMT= "$UPDATE_STMT

UPDATE_STMT=`echo UPDATE ${CHLD_TBL_NM} FROM ${PRNT_TBL_NM}_UPD_VT ${PRNT_TBL_NM} SET RCRD_STTS_CD = "'DEL'", UPDTD_LOAD_LOG_KEY = POST_RI_EDW_LOAD_LOG_VT.LOAD_LOG_KEY WHERE ${JOINCNDTN1} AND ${CHLD_TBL_NM}.RCRD_STTS_CD ="'ACT'"\;`
echo "UPDATE_STMT= "$UPDATE_STMT
echo "\n "

############Removal of Text files which have been generated in beginning#############
Files_Count=`ls -lrt $PMDIR/SrcFiles/Sample_ri_chck_*.txt | wc -l `

echo 'Text File Count ='$Files_Count

rm -f $PMDIR/SrcFiles/Sample_ri_chck_*.txt


bteq <<EOF

.SET WIDTH 150;

/*********************************************************************************************
put BTEQ in Transaction mode
*********************************************************************************************/

.SET SESSION TRANSACTION BTET;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

/**************************************************************************************************

**************************************************************************************************/

.run file $LOGON/$LOGON_ID;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

SELECT SESSION;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

SET QUERY_BAND = 'ApplicationName=$0;Frequency=weekly;' FOR SESSION;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

/**************************************************************************************************
Set default database based on parmeter in parm file if needs to be different than the logon ID's 
default database.
Set default database derived from parameter file
**************************************************************************************************/

DATABASE $ETL_VIEWS_DB;

/************************************** Error Handling *******************************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/************************************** Error Handling *******************************************/

.SET WIDTH 2000;

$POST_RI_EDW_LLK_VT

$EDW_LLK_VT

$PRNT_TBL_VT

$COLSTATS_PRNT
$CHLD_TBL_VT
$COLSTATS_CHLD

$FINAL_TBL_VT
$COLSTATS_FNL
$UPDATE_STMT

/***************** Error Handling ********************************/
.IF ERRORCODE <> 0 THEN .GOTO ERRORS
/***************** Error Handling ********************************/

.EXPORT RESET

.LABEL ERRORS
.QUIT ERRORCODE
EOF

#===============================================================================
#END BTEQ EXECUTION
#===============================================================================
# show AIX return code and exit with it
RETURN_CODE=$?

###########################################################################
# Functionality to determine the run time
###########################################################################

END_TIME=`date +%s`
TT_SECS=$(( END_TIME - ST_TIME))
TT_HRS=$(( TT_SECS / 3600 ))
TT_REM_MS=$(( TT_SECS % 3600 ))
TT_MINS=$(( TT_REM_MS / 60 ))
TT_REM_SECS=$(( TT_REM_MS % 60 ))
echo "Total Time Taken="$TT_HRS:$TT_MINS:$TT_REM_SECS HH:MM:SS
echo "script return code="$RETURN_CODE
exit $RETURN_CODE

