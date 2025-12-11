#!/bin/bash

# This script is used to generate a summary TSV file from one to the reports subdirectories
# in the top-level collectionAuditing directory.

 ## Determine where the collectionAuditing directory is located and change to that directory.
if [ -d "$HOME/work/collectionAuditing" ]; then
    cd "$HOME/work/collectionAuditing"
elif [ -d "/arc/projects/CADC/collectionAuditing" ]; then
    cd "/arc/projects/CADC/collectionAuditing"
else
    echo "Unable to determine the location of the collectionAuditing directory."
    exit 1
fi

# Check that one and only one subdirectory is provided on the command line.
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <reports_subdirectory>"
    echo "Example: $1 typeProfiles_reports"
    exit 1
fi

# Check that the provided subdirectory exists.
if [ ! -d "$1" ]; then
    echo "The specified subdirectory '$1' does not exist."
    exit 1
fi

# Set the reports subdirectory variable and chceck that is is readable and not empty
REPORTS_SUBDIR="$1"
if [ ! -r "$REPORTS_SUBDIR" ] || [ -z "$(ls -A "$REPORTS_SUBDIR")" ]; then
    echo "The specified subdirectory '$REPORTS_SUBDIR' is not readable or is empty."
    exit 1
fi

# Create the summary TSV filename based on the reports subdirectory name and stripping off the "_reports" suffix.
SUMMARY_TSV="${REPORTS_SUBDIR/"_reports"}_summary.tsv"
echo "Generating summary file: $SUMMARY_TSV"

# Grep the content for all the .tsv files in the reports subdirectory and combine them into the summary TSV file.
# The relevant content is in the last 3 lines of the files.
tail -q -n 3 "$REPORTS_SUBDIR"/*.tsv | sort -u > "$SUMMARY_TSV"
