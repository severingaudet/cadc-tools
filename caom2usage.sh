#!/bin/bash

cadc-get-cert -n -q 
ARGUS="https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"
DATE=`date "+%Y-%m-%dT%H:%M:%S"`
CADC_TAP="/Users/gaudet_1/.pyenv/shims/cadc-tap query -q --timeout=15 -a -k -m 1 -s $ARGUS"

declare -a collectionInstrument=(
    "BRITE-Constellation,BRITE-AUSTRIA"
#    "BRITE-Constellation,BRITE-HEWELIUSZ"
#    "BRITE-Constellation,BRITE-LEM"
#    "BRITE-Constellation,BRITE-Toronto"
#    "BRITE-Constellation,UniBRITE"
)

declare -a obsFields=(
    "accMetaChecksum"
    "algorithm_name"
    "collection"
    "environment_ambientTemp"
    "environment_elevation"
    "environment_humidity"
    "environment_photometric"
    "environment_seeing"
    "environment_tau"
    "environment_wavelengthTau"
    "instrument_keywords"
    "instrument_name"
    "intent"
    "lastModified"
    "maxLastModified"
    "members"
    "metaChecksum"
    "metaProducer"
    "metaReadGroups"
    "metaRelease"
    "obsID"
    "observationID"
    "observationURI"
    "proposal_id"
    "proposal_keywords"
    "proposal_pi"
    "proposal_project"
    "proposal_title"
    "requirements_flag"
    "sequenceNumber"
    "targetPosition_coordinates_cval1"
    "targetPosition_coordinates_cval2"
    "targetPosition_coordsys"
    "targetPosition_equinox"
    "target_keywords"
    "target_moving"
    "target_name"
    "target_redshift"
    "target_standard"
    "target_targetID"
    "target_type"
    "telescope_geoLocationX"
    "telescope_geoLocationY"
    "telescope_geoLocationZ"
    "telescope_keywords"
    "telescope_name"
    "type"
    "typeCode"
)

declare -a planeFields=(
    "accMetaChecksum"
    "calibrationLevel"
    "creatorID"
    "custom_bounds"
    "custom_bounds_lower"
    "custom_bounds_samples"
    "custom_bounds_upper"
    "custom_bounds_width"
    "custom_ctype"
    "custom_dimension"
    "dataProductType"
    "dataReadGroups"
    "dataRelease"
    "energy_bandpassName"
    "energy_bounds"
    "energy_bounds_lower"
    "energy_bounds_samples"
    "energy_bounds_upper"
    "energy_bounds_width"
    "energy_dimension"
    "energy_emBand"
    "energy_energyBands"
    "energy_freqSampleSize"
    "energy_freqWidth"
    "energy_resolvingPower"
    "energy_resolvingPowerBounds"
    "energy_restwav"
    "energy_sampleSize"
    "energy_transition_species"
    "energy_transition_transition"
    "lastModified"
    "maxLastModified"
    "metaChecksum"
    "metaProducer"
    "metaReadGroups"
    "metaRelease"
    "metrics_background"
    "metrics_backgroundStddev"
    "metrics_fluxDensityLimit"
    "metrics_magLimit"
    "metrics_sourceNumberDensity"
    "obsID"
    "observable_ucd"
    "planeID"
    "planeURI"
    "polarization_dimension"
    "polarization_states"
    "position_bounds"
    "position_bounds_samples"
    "position_bounds_size"
    "position_dimension_naxis1"
    "position_dimension_naxis2"
    "position_resolution"
    "position_resolutionBounds"
    "position_sampleSize"
    "position_timeDependent"
    "productID"
    "provenance_inputs"
    "provenance_keywords"
    "provenance_lastExecuted"
    "provenance_name"
    "provenance_producer"
    "provenance_project"
    "provenance_reference"
    "provenance_runID"
    "provenance_version"
    "publisherID"
    "quality_flag"
    "time_bounds"
    "time_bounds_lower"
    "time_bounds_samples"
    "time_bounds_upper"
    "time_bounds_width"
    "time_dimension"
    "time_exposure"
    "time_resolution"
    "time_resolutionBounds"
    "time_sampleSize"
)

#
# Execute Observation queries
#

queryPlaneFieldNotNull () {
    COLLECTION="$1"
    INSTRUMENT_NAME="$2"
    FIELD="$3"
    PLANE_TOTAL="$4"
    NUM_NOT_NULL=0
    QUERY="select count(*) from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"' and P.$FIELD is not null"
    NUM_NOT_NULL=$($CADC_TAP "$QUERY")
    PERCENTAGE_NOT_NULL=$(echo "scale=2; $NUM_NOT_NULL / $PLANE_TOTAL * 100" | bc)
    echo "$COLLECTION,$INSTRUMENT_NAME,'Plane',$FIELD,$NUM_NOT_NULL,$PERCENTAGE_NOT_NULL"
}

queryPlane () {
    COLLECTION="$1"
    INSTRUMENT_NAME="$2"
    PLANE_TOTAL=0
    QUERY="select count(*) 
        from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"'"
    PLANE_TOTAL=$($CADC_TAP "$QUERY")
    echo "$COLLECTION" "$INSTRUMENT_NAME" "$FIELD" "$PLANE_TOTAL"
    for FIELD in "${planeFields[@]}"
    do
        queryPlaneFieldNotNull "$COLLECTION" "$INSTRUMENT_NAME" "$FIELD" "$PLANE_TOTAL"
    done
}

queryObsFieldNotNull () {
    COLLECTION="$1"
    INSTRUMENT_NAME="$2"
    FIELD="$3"
    TOTAL="$4"
    QUERY="select count(*) 
        from caom2.Observation
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"' and '"$FIELD"' is not null"
    NUM_NOT_NULL=$($CADC_TAP "$QUERY")
    PERCENTAGE_NOT_NULL=$(echo "scale=2; $NUM_NOT_NULL / $TOTAL * 100" | bc)
    echo "$COLLECTION,$_NAME,'Observation',$FIELD,$NUM_NOT_NULL,$PERCENTAGE_NOT_NULL"
}

queryObservation () {
    COLLECTION="$1"
    INSTRUMENT_NAME="$2"
    QUERY="select count(*)
        from caom2.Observation 
        where collection = '"$COLLECTION"' and instrument_name = '"$INSTRUMENT_NAME"'"
    OBS_TOTAL=$($CADC_TAP "$QUERY")
    for FIELD in "${obsFields[@]}"
    do
        queryObsFieldNotNull "$COLLECTION" "$INSTRUMENT_NAME" "$FIELD" "$OBS_TOTAL"
    done
}

for COLLECTION_INSTRUMENT in "${collectionInstrument[@]}"
do
    IFS=',' read -r -a CI <<< "$COLLECTION_INSTRUMENT"
#    queryObservation "${CI[0]}" "${CI[1]}"
    queryPlane "${CI[0]}" "${CI[1]}"
done