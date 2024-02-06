#!/bin/bash

download_dir () {
    SRC_DIR="vault:adass2022/uploads/$1"
    DEST_DIR="$HOME/work/git/ADASSProceedings2022/papers_temp/$1"
    echo $DEST_DIR
    
    if [ ! -d "$DEST_DIR" ]
    then
    	mkdir $DEST_DIR
    else
    	rm -rf $DEST_DIR/*
    fi

    #
    # Do not copy files created by the ADASS account adass2022_b57 and any
    # Powerpoint, PDF, MP4 or AVI files that may have been uploaded.
    #

    NO_FILES=true
    FIRST_FILE=true
    BUNDLE_COPIED=false
    TIME_ORDERED_FILE_LIST=$(vls -lt $SRC_DIR | grep -v "   adass2022_b57" | grep -v '.pptx' | grep -v '.mp4' | grep -v '.avi' | awk '{print $NF}' ) 

    for FILE in $TIME_ORDERED_FILE_LIST
    do
	#
	# If the first/most recent file is a tar, zip, rar or 7z file, download only it and not any others.
	#

	if ( [[ "$FILE" == *".tar.gz" ]] || [[ "$FILE" == *".tar" ]] || [[ "$FILE" == *".zip" ]] || [[ "$FILE" == *".rar" ]] || [[ "$FILE" == *".7z" ]] ); then
	    BUNDLE_FILE=true
	else
	    BUNDLE_FILE=false
	fi

	if ( [ "$BUNDLE_FILE" = true ] ) ; then
	    if ( [ "$FIRST_FILE" = true ] ) ; then
		echo "    copy bundle $FILE"
		vcp $SRC_DIR/$FILE $DEST_DIR/$FILE
		NO_FILES=false
		BUNDLE_COPIED=true
	    else
		echo "    warning: not copying older bundle $FILE"
	    fi
	elif $BUNDLE_COPIED ; then
	    echo "    warning: not copying older file $FILE"
	else
	    echo "    copy $FILE"
	    vcp $SRC_DIR/$FILE $DEST_DIR/$FILE
	    NO_FILES=false
	fi
	FIRST_FILE=false
    done

    #
    # If no files have been copied, delete the destination directory
    #

    if [ "$NO_FILES" = true ]
    then
        echo "    Removing empty $DEST_DIR"
        rmdir $DEST_DIR
    fi
}

DATE=`date "+%Y-%m-%dT%H:%M:%S"`

cadc-get-cert -n

if [ $# -eq 0 ]
then
    DIRS=$(vls vault:adass2022/uploads)
else
    DIRS="$@"
fi

for DIR in $DIRS
do
    download_dir $DIR
done
