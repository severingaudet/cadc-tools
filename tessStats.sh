#!/bin/bash

if [ $# -gt 0 ]; then
    echo "Usage: tessStats"
    exit 255
fi

cadc-get-cert --netrc ~/.netrc

# Set up TAP variables

TAP_PARAMS="LANG=ADQL&FORMAT=TSV"
MAST_TAP_URL="http://vao.stsci.edu/CAOMTAP/TapService.aspx/sync"
AMS_TAP_SERVICE="ivo://cadc.nrc.ca/ams/mast"
CVO_TAP_SERVICE="ivo://cadc.nrc.ca/argus"
SKIP_TAP_SERVICE="ivo://cadc.nrc.ca/ams/mast"

AMS_TAP_URL="http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/ams/mast/auth-sync"
CVO_TAP_URL="http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap"
SKIP_TAP_URL="http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/ams/mast/auth-sync"
AD_TAP_URL="http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/ad/auth-sync"

DATE=`date "+%Y-%m-%d"`
DATE_TRY_AFTER=`date -u "+%Y-%m-%d %H:%M:%S"`
DATE_CAOM_INGEST_AFTER=`date -v -24H -u "+%Y-%m-%dT%H:%M:%S"`

#
# Query CAOM for Observations
#

echo "Querying for total Observations"
QUERY="QUERY=select '"$DATE"' , 'MAST', count(*) from dbo.caomObservation where collection = 'TESS'"
curl $MAST_TAP_URL -s -n -L -d "LANG=ADQL&FORMAT=CSV" --data-urlencode "$QUERY" | grep -v COUNT_ALL | grep MAST | sed 's/\,/	/g'

QUERY="select '"$DATE"' as date, 'AMS ', count(*) as numObservations from caom2.Observation where collection = 'TESS'"
#echo "cadc-tap query -n -q  -s $AMS_TAP_SERVICE -f tsv $QUERY"
cadc-tap query -n -q  -s $AMS_TAP_SERVICE -f tsv "$QUERY"

QUERY="select '"$DATE"' as date, 'CVO ', count(*) as numObservations from caom2.Observation where collection = 'TESS'"
cadc-tap query -n -q  -s $CVO_TAP_SERVICE -f tsv "$QUERY"

echo "Querying CAOM for HST Observations with null metaRelease dates"
QUERY="select '"$DATE"' as date, count(*) as numObservations from caom2.Observation where collection = 'TESS' and metaRelease is null"
cadc-tap query -n -q  -s $AMS_TAP_SERVICE -f tsv "$QUERY"

echo ""
echo "Querying CAOM for TESS Observations harvested since $DATE_CAOM_INGEST_AFTER"
QUERY="QUERY=select count(*) as numObservations from caom2.Observation where collection = 'TESS' and maxLastModified >= '"$DATE_CAOM_INGEST_AFTER"'"
curl $AMS_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"

#
# Query Skip table for observations
#

echo ""
echo "Querying skip table for all Observation todo entries"
QUERY="QUERY=select '"$DATE"' as date, count(*) as numToDo from caom2.HarvestSkipURI where cname = 'Observation' and source like '%TESS%'"
curl $SKIP_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"

echo "Querying skip table for Observation todo entries with tryAfter <= $DATE_TRY_AFTER UTC"
QUERY="QUERY=select '"$DATE"' as date, count(*) as numToDo from caom2.HarvestSkipURI where cname = 'Observation' and tryAfter <= '"$DATE_TRY_AFTER"' and source like '%TESS%'"
curl $SKIP_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"

echo "Querying skip table for Observation todo entries with tryAfter > $DATE_TRY_AFTER UTC"
QUERY="QUERY=select '"$DATE"' as date, count(*) as numToDo from caom2.HarvestSkipURI where cname = 'Observation' and tryAfter > '"$DATE_TRY_AFTER"' and source like '%TESS%'"
curl $SKIP_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"

echo "Querying skip table for Observation sync errors"
QUERY="QUERY=select '"$DATE"' as date, count(*) as numErrorObservation from caom2.HarvestSkipURI where errorMessage is not null and cname = 'Observation' and source like '%TESS%'"
curl $SKIP_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"

echo "Querying skip table for Observation sync errors"
QUERY="QUERY=select count(*) as numFailed, substring(errorMessage,1,40) as msg, min(skipID) as example, min(tryAfter) as minTryAfter, max(tryAfter) as maxTryAfter from caom2.HarvestSkipURI where cname='Observation' and errorMessage is not null and source like '%TESS%' group by msg order by msg"
curl $SKIP_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"

#echo "Dumping Observation sync errors to file"
#QUERY="QUERY=select * from caom2.HarvestSkipURI where cname='Observation' and errorMessage is not null and source like '%TESS%' order by skipID"
#curl $SKIP_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY" > TESS-Observation-errors.tsv

#
# Query CAOM for artifacts
#

echo ""
echo "Querying CAOM for all artifacts matching mast:TESS/% path"
QUERY="QUERY=select '"$DATE"' as date, count(*) as numFiles, sum(contentLength/1024.0/1024.0/1024.0/1024.0) as TiB from caom2.Plane as P join caom2.Artifact as A on P.planeID = A.planeID where uri like 'mast:TESS/%'"
curl $AMS_TAP_URL -n -L -d "$TAP_PARAMS" --data-urlencode "$QUERY"
