#!/bin/bash

ROOT=$HOME/work/git/ADASSProceedings2022/
TEMP_DIR=papers_temp
DEST_DIR=papers

# Loop through files in both TEMP and DEST and compare

echo "Ignoring *.tar.gz *.tar *.zip and *.rar files"

cd $ROOT/$TEMP_DIR
for t in * ; do
    #
    # Create directory in DEST in not there
    #
    
    if [ ! -d $ROOT/$DEST_DIR/$t ] ; then
        echo "mkdir $DEST_DIR/$t"
        mkdir $ROOT/$DEST_DIR/$t
    fi
    
    #
    # Copy missing files, ignoring bundle files (tar, zip and rar).
    #
    
    for tf in "$t"/* ; do
        if [[ "$tf" == *tar* ]] ; then
            continue
        elif [[ "$tf" == *.rar ]] ; then
            continue
        elif [[ "$tf" == *.zip ]] ; then
            continue
        elif [ -f $ROOT/$DEST_DIR/$tf ] ; then
            cmp $tf $ROOT/$DEST_DIR/$tf
        else
            echo "cp $tf to $DEST_DIR/$tf"
            cp $tf $ROOT/$DEST_DIR/$tf
        fi
    done
done

cd $ROOT/$DEST_DIR
for d in * ; do
    if [ -d $ROOT/$TEMP_DIR/$d ] ; then
#        echo $d
        for df in "$d"/* ; do
            if [ ! -f $ROOT/$TEMP_DIR/$df ] ; then
                echo "rm $DEST_DIR/$df"
#                rm $DEST_DIR/$df
            fi
        done
    fi
done
