#!/bin/bash

cadc-get-cert -n -q 
ARGUS="https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"
DATE=`date "+%Y-%m-%dT%H:%M:%S"`
CADC_TAP="/Users/gaudet_1/.pyenv/shims/cadc-tap query -q --timeout=15 -a -k -s $ARGUS"
OBS_FILENAME="collectionInstrumentTotalObs.csv"
PLANES_FILENAME="collectionInstrumentTotalPlanes.csv"

declare -a obsFields=(
    "caom2.Observation.accMetaChecksum"
    "caom2.Observation.algorithm_name"
    "caom2.Observation.collection"
    "caom2.Observation.environment_ambientTemp"
    "caom2.Observation.environment_elevation"
    "caom2.Observation.environment_humidity"
    "caom2.Observation.environment_photometric"
    "caom2.Observation.environment_seeing"
    "caom2.Observation.environment_tau"
    "caom2.Observation.environment_wavelengthTau"
    "caom2.Observation.instrument_keywords"
    "caom2.Observation.instrument_name"
    "caom2.Observation.intent"
    "caom2.Observation.lastModified"
    "caom2.Observation.maxLastModified"
    "caom2.Observation.members"
    "caom2.Observation.metaChecksum"
    "caom2.Observation.metaProducer"
    "caom2.Observation.metaReadGroups"
    "caom2.Observation.metaRelease"
    "caom2.Observation.obsID"
    "caom2.Observation.observationID"
    "caom2.Observation.observationURI"
    "caom2.Observation.proposal_id"
    "caom2.Observation.proposal_keywords"
    "caom2.Observation.proposal_pi"
    "caom2.Observation.proposal_project"
    "caom2.Observation.proposal_title"
    "caom2.Observation.requirements_flag"
    "caom2.Observation.sequenceNumber"
    "caom2.Observation.targetPosition_coordinates_cval1"
    "caom2.Observation.targetPosition_coordinates_cval2"
    "caom2.Observation.targetPosition_coordsys"
    "caom2.Observation.targetPosition_equinox"
    "caom2.Observation.target_keywords"
    "caom2.Observation.target_moving"
    "caom2.Observation.target_name"
    "caom2.Observation.target_redshift"
    "caom2.Observation.target_standard"
    "caom2.Observation.target_targetID"
    "caom2.Observation.target_type"
    "caom2.Observation.telescope_geoLocationX"
    "caom2.Observation.telescope_geoLocationY"
    "caom2.Observation.telescope_geoLocationZ"
    "caom2.Observation.telescope_keywords"
    "caom2.Observation.telescope_name"
    "caom2.Observation.type"
    "caom2.Observation.typeCode"
)

declare -a planeFields=(
    "caom2.Plane.accMetaChecksum"
    "caom2.Plane.calibrationLevel"
    "caom2.Plane.creatorID"
    "caom2.Plane.custom_bounds"
    "caom2.Plane.custom_bounds_lower"
    "caom2.Plane.custom_bounds_samples"
    "caom2.Plane.custom_bounds_upper"
    "caom2.Plane.custom_bounds_width"
    "caom2.Plane.custom_ctype"
    "caom2.Plane.custom_dimension"
    "caom2.Plane.dataProductType"
    "caom2.Plane.dataReadGroups"
    "caom2.Plane.dataRelease"
    "caom2.Plane.energy_bandpassName"
    "caom2.Plane.energy_bounds"
    "caom2.Plane.energy_bounds_lower"
    "caom2.Plane.energy_bounds_samples"
    "caom2.Plane.energy_bounds_upper"
    "caom2.Plane.energy_bounds_width"
    "caom2.Plane.energy_dimension"
    "caom2.Plane.energy_emBand"
    "caom2.Plane.energy_energyBands"
    "caom2.Plane.energy_freqSampleSize"
    "caom2.Plane.energy_freqWidth"
    "caom2.Plane.energy_resolvingPower"
    "caom2.Plane.energy_resolvingPowerBounds"
    "caom2.Plane.energy_restwav"
    "caom2.Plane.energy_sampleSize"
    "caom2.Plane.energy_transition_species"
    "caom2.Plane.energy_transition_transition"
    "caom2.Plane.lastModified"
    "caom2.Plane.maxLastModified"
    "caom2.Plane.metaChecksum"
    "caom2.Plane.metaProducer"
    "caom2.Plane.metaReadGroups"
    "caom2.Plane.metaRelease"
    "caom2.Plane.metrics_background"
    "caom2.Plane.metrics_backgroundStddev"
    "caom2.Plane.metrics_fluxDensityLimit"
    "caom2.Plane.metrics_magLimit"
    "caom2.Plane.metrics_sourceNumberDensity"
    "caom2.Plane.obsID"
    "caom2.Plane.observable_ucd"
    "caom2.Plane.planeID"
    "caom2.Plane.planeURI"
    "caom2.Plane.polarization_dimension"
    "caom2.Plane.polarization_states"
    "caom2.Plane.position_bounds"
    "caom2.Plane.position_bounds_samples"
    "caom2.Plane.position_bounds_size"
    "caom2.Plane.position_dimension_naxis1"
    "caom2.Plane.position_dimension_naxis2"
    "caom2.Plane.position_resolution"
    "caom2.Plane.position_resolutionBounds"
    "caom2.Plane.position_sampleSize"
    "caom2.Plane.position_timeDependent"
    "caom2.Plane.productID"
#    "caom2.Plane.provenance_inputs"
    "caom2.Plane.provenance_keywords"
    "caom2.Plane.provenance_lastExecuted"
    "caom2.Plane.provenance_name"
    "caom2.Plane.provenance_producer"
    "caom2.Plane.provenance_project"
    "caom2.Plane.provenance_reference"
    "caom2.Plane.provenance_runID"
    "caom2.Plane.provenance_version"
    "caom2.Plane.publisherID"
    "caom2.Plane.quality_flag"
    "caom2.Plane.time_bounds"
    "caom2.Plane.time_bounds_lower"
    "caom2.Plane.time_bounds_samples"
    "caom2.Plane.time_bounds_upper"
    "caom2.Plane.time_bounds_width"
    "caom2.Plane.time_dimension"
    "caom2.Plane.time_exposure"
    "caom2.Plane.time_resolution"
    "caom2.Plane.time_resolutionBounds"
    "caom2.Plane.time_sampleSize"
)

#
# Generic queries for Observations and Planes used by both looping by COLLECTION, INSTRUMENT_NAME or by FIELD
#

processObservationFields () {
    for FIELD in "${obsFields[@]}" 
    do
        FILENAME="$DATE-$FIELD.csv"
        SUMMARY_TOTAL_OBS=0
        SUMMARY_NUM_NOT_NULL=0

        while IFS="," read -r COLLECTION INSTRUMENT_NAME TOTAL_OBS
        do
            echo "$FIELD $COLLECTION $INSTRUMENT_NAME $TOTAL_OBS"
            QUERY="select count(*)
                    from caom2.Observation
                    where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"' and $FIELD is not null"
            NUM_NOT_NULL=$($CADC_TAP "$QUERY")

            PERCENTAGE_NOT_NULL=$(echo "scale=2; $NUM_NOT_NULL / $TOTAL_OBS * 100" | bc)
            SUMMARY_TOTAL_OBS=$(($SUMMARY_TOTAL_OBS + $TOTAL_OBS))
            SUMMARY_NUM_NOT_NULL=$(($SUMMARY_NUM_NOT_NULL + $NUM_NOT_NULL))

            echo "$FIELD,$COLLECTION,$INSTRUMENT_NAME,$TOTAL_OBS,$NUM_NOT_NULL,$PERCENTAGE_NOT_NULL" >> $FILENAME
        done < $OBS_FILENAME

        SUMMARY_PERCENTAGE_NOT_NULL=$(echo "scale=6; $SUMMARY_NUM_NOT_NULL / $SUMMARY_TOTAL_OBS * 100" | bc)
        echo "$FIELD,SUMMARY, ,$SUMMARY_TOTAL_OBS,$SUMMARY_NUM_NOT_NULL,$SUMMARY_PERCENTAGE_NOT_NULL" >> $FILENAME
    done
}

processPlaneFields () {
    for FIELD in "${planeFields[@]}" 
    do   
        FILENAME="$DATE-$FIELD.csv"
        SUMMARY_TOTAL_PLANES=0
        SUMMARY_NUM_NOT_NULL=0

        while IFS="," read -r COLLECTION INSTRUMENT_NAME TOTAL_PLANES
        do
            echo "$FIELD $COLLECTION $INSTRUMENT_NAME $TOTAL_PLANES"
            QUERY="select count(*) 
                from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
                where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"' and $FIELD is not null"
            NUM_NOT_NULL=$($CADC_TAP "$QUERY")

            PERCENTAGE_NOT_NULL=$(echo "scale=2; $NUM_NOT_NULL / $TOTAL_PLANES * 100" | bc)
            SUMMARY_TOTAL_PLANES=$(($SUMMARY_TOTAL_PLANES + $TOTAL_PLANES))
            SUMMARY_NUM_NOT_NULL=$(($SUMMARY_NUM_NOT_NULL + $NUM_NOT_NULL))

            echo "$FIELD,$COLLECTION,$INSTRUMENT_NAME,$TOTAL_PLANES,$NUM_NOT_NULL,$PERCENTAGE_NOT_NULL" >> $FILENAME


            queryPlaneFieldNotNull "$FIELD" "$COLLECTION" "$INSTRUMENT_NAME" "$TOTAL_PLANES" 
        done < $PLANES_FILENAME

        SUMMARY_PERCENTAGE_NOT_NULL=$(echo "scale=6; $SUMMARY_NUM_NOT_NULL / $SUMMARY_TOTAL_PLANES * 100" | bc)
        echo "$FIELD,SUMMARY, ,$SUMMARY_TOTAL_PLANES,$SUMMARY_NUM_NOT_NULL,$SUMMARY_PERCENTAGE_NOT_NULL" >> $FILENAME
    done
}

queryObsFieldNotNull () {
    FIELD="$1"
    COLLECTION="$2"
    INSTRUMENT_NAME="$3"
    TOTAL_OBS="$4"

    QUERY="select count(*)
        from caom2.Observation
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"' and $FIELD is not null"
    NUM_NOT_NULL=$($CADC_TAP "$QUERY")

    PERCENTAGE_NOT_NULL=$(echo "scale=2; $NUM_NOT_NULL / $TOTAL_OBS * 100" | bc)
    SUMMARY_TOTAL_OBS=$(($SUMMARY_TOTAL_OBS + $TOTAL_OBS))
    SUMMARY_NUM_NOT_NULL=$(($SUMMARY_NUM_NOT_NULL + $NUM_NOT_NULL))

    echo "$FIELD,$COLLECTION,$INSTRUMENT_NAME,$TOTAL_OBS,$NUM_NOT_NULL,$PERCENTAGE_NOT_NULL" >> $FILENAME
}

queryPlaneFieldNotNull () {
    FIELD="$1"
    COLLECTION="$2"
    INSTRUMENT_NAME="$3"
    TOTAL_PLANES="$4"
    FILENAME="$DATE-$FIELD.csv"
    NUM_NOT_NULL=0
    QUERY="select count(*) 
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"' and $FIELD is not null"
    NUM_NOT_NULL=$($CADC_TAP "$QUERY")
    PERCENTAGE_NOT_NULL=$(echo "scale=2; $NUM_NOT_NULL / $TOTAL_PLANES * 100" | bc)
    echo "$FIELD,$COLLECTION,$INSTRUMENT_NAME,$TOTAL_PLANES,$NUM_NOT_NULL,$PERCENTAGE_NOT_NULL" >> $FILENAME
}

#
# Functions for looping by COLLECTION, INSTRUMENT_NAME
#

queryPlaneByCollection () {
    COLLECTION="$1"
    INSTRUMENT_NAME="$2"
    QUERY="select count(*) 
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"'"
    TOTAL_PLANES=$($CADC_TAP "$QUERY")
    for FIELD in "${planeFields[@]}"
    do
        queryPlaneFieldNotNull "$COLLECTION" "$INSTRUMENT_NAME" "$TOTAL_PLANES" "$FIELD"
    done
}

queryObsByCollection () {
    COLLECTION="$1"
    INSTRUMENT_NAME="$2"
    QUERY="select count(*)
        from caom2.Observation 
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"'"
    TOTAL_OBS=$($CADC_TAP "$QUERY")
    for FIELD in "${obsFields[@]}"
    do
        queryObsFieldNotNull "$COLLECTION" "$INSTRUMENT_NAME" "$TOTAL_OBS" "$FIELD" 
    done
}

queryByCollection () {
    for COLLECTION_INSTRUMENT in "${collectionInstrument[@]}"
    do
        IFS=',' read -r -a CI <<< "$COLLECTION_INSTRUMENT"
        queryObsByCollection "${CI[0]}" "${CI[1]}"
        queryPlaneByCollection "${CI[0]}" "${CI[1]}"
    done
}

#
# Functions for looping by FIELD instead of by COLLECTION, INSTRUMENT_NAME. This is less efficient but provides
# a better view of the data.
#

queryObsByField () {
    FIELD="$1"
    for COLLECTION_INSTRUMENT in "${collectionInstrument[@]}"
    do
        IFS=',' read -r -a CI <<< "$COLLECTION_INSTRUMENT"
        COLLECTION="${CI[0]}"
        INSTRUMENT_NAME="${CI[1]}"
        QUERY="select count(*)
            from caom2.Observation 
            where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"'"
        TOTAL_OBS=$($CADC_TAP "$QUERY")
        queryObsFieldNotNull "$COLLECTION" "$INSTRUMENT_NAME" "$TOTAL_OBS" "$FIELD" 
    done
}

queryPlaneByField () {
    FIELD="$1"
    for COLLECTION_INSTRUMENT in "${collectionInstrument[@]}"
    do
        IFS=',' read -r -a CI <<< "$COLLECTION_INSTRUMENT"
        COLLECTION="${CI[0]}"
        INSTRUMENT_NAME="${CI[1]}"
        QUERY="select count(*) 
            from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
             where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"'"
        TOTAL_PLANES=$($CADC_TAP "$QUERY")
        queryPlaneFieldNotNull "$COLLECTION" "$INSTRUMENT_NAME" "$TOTAL_PLANES" "$FIELD"
    done
}

queryByField () {
    for FIELD in "${obsFields[@]}"
    do
        queryObsByField "$FIELD"
    done
    for FIELD in "${planeFields[@]}"
    do
        queryPlaneByField "$FIELD"
    done
}

processObservationFields
processPlaneFields