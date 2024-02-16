#!/bin/bash

TEMP_DIR=$HOME/work/git/ADASSProceedings2022/papers_temp

# Extract files from tar, zip and rar archives

cd $TEMP_DIR
for i in *
do
    cd $TEMP_DIR/$i
    if compgen -G "*.tar" > /dev/null ; then
        echo "Unpacking $i/*.tar"
        tar xf *.tar --exclude "__MACOSX"
    elif compgen -G "*.tar.gz" > /dev/null ; then
        echo "Unpacking $i/*.tar.gx"
        tar xzf *.tar.gz --exclude "__MACOSX"
    elif compgen -G "*.zip" > /dev/null ; then
        echo "Unpacking $i/*.zip"
        unzip -q -o *.zip -x '__MACOSX*'
    elif compgen -G "*.rar" > /dev/null ; then
        echo "Unpacking $i/*.rar"
        unrar e -idq -o+ *.rar
    fi
done

# Move contents of subdirectories to main paper directory

echo "mv contents of subdirectories to main paper directory"
cd $TEMP_DIR
for i in * ; do
    cd $TEMP_DIR/$i
    for j in "$i"* ; do
        if [ -d $j ] ; then
            echo "mv $i/$j/* to $i"
            mv $j/* .
            rm -f $j/.DS_Store
            rmdir $j
        fi
    done
done

cd $TEMP_DIR
echo "mv I03/Figures/* to I03"
mv I03/Figures/* I03; rm -rf I03/Figures
echo "mv P77/figs/* P77"
mv P77/figs/* P77; rm -rf P77/figs
echo "mv I12/Users/anna.anku/OneDrive\ -\ ESA/Desktop/I12/* to I12"
mv I12/Users/anna.anku/OneDrive\ -\ ESA/Desktop/I12/* I12; rm -rf I12/Users
echo "mv C03/ADASS22_C03_ESASky_final/* to C03"
mv C03/ADASS22_C03_ESASky_final/* C03; rm -rf C03/ADASS22_C03_ESASky_final
echo "mv P44/ADASS22_P44_JWST_Datalabs_poster_final/* to P44"
mv P44/ADASS22_P44_JWST_Datalabs_poster_final/* P44; rm -rf P44/ADASS22_P44_JWST_Datalabs_poster_final

# other fixes

echo "mv B02/makedef B02/makedefs"
mv B02/makedef B02/makedefs

echo "rm P44/ADASS22_P44_JWST_Datalabs_poster.pdf"
rm -f P44/ADASS22_P44_JWST_Datalabs_poster.pdf
echo "rm P18/A_SKY_PORTAL_FOR_EUCLID_DATA__THE_EUCLID_SCIENTIFIC_ARCHIVE_SYSTEM.pdf"
rm -f P18/A_SKY_PORTAL_FOR_EUCLID_DATA__THE_EUCLID_SCIENTIFIC_ARCHIVE_SYSTEM.pdf
echo "rm P18/P18_slides.pdf"
rm -f P18/P18_slides.pdf
echo "rm P18/P18_poster.pdf.pdf"
rm -f P18/P18_poster.pdf.pdf
echo "rm P27/P27_poster.pdf"
rm -f P27/P27_poster.pdf
echo "rm P24/ADASS_template.tex"
rm -f P24/ADASS_template.tex
echo "rm P68/P68_poster.pdf"
rm -f P68/P68_poster.pdf
echo "rm B07/B07_slides.pdf"
rm -f B07/B07_slides.pdf
echo "rm I08/LICENSE"
rm -f I08/LICENSE
echo "rm I08/oldauthors"
rm -f I08/oldauthors
echo "rm I08/Makefile"
rm -f I08/Makefile
echo "rm I10/ADASS.pdf"
rm -f I10/ADASS.pdf

echo "mv P60_v2 -> P60"
for i in $TEMP_DIR/P60/* ; do
    j=`echo $i | sed 's/_v2//g'`
    mv $i $j
done

# Remove all user generated <PID>.pdf

echo "rm <PID>/<PID>.pdf files"
cd $TEMP_DIR
for i in * ; do
    if [ -e $i/$i.pdf ] ; then
        echo "rm $i/$i.pdf"
        rm $i/$i.pdf
    fi
done

# Removing unnecessary extensions

cd $TEMP_DIR
find * -name "*slides*.pdf" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*sliides*.pdf" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*lightning*.pdf" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*poster*pdf" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.bst" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.sty" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.aux" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.log" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.dvi" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.blg" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.bbl" -exec echo "rm {}" \; -exec rm {} \;
find * -name "*.yaml" -exec echo "rm {}" \; -exec rm {} \;

# List all extensions to verify

echo "Listing all unnecessary extensions"
cd $TEMP_DIR
find * -type f -print | grep -v ".tex" | grep -v ".bib" | grep -v ".eps" | grep -vw makedefs | grep -vi copyright | grep -v ".tar*" | grep -v ".zip" | grep -v ".rar"

#awk -F\. 'NF>1{print $NF}' | sort -u

# Now sync from papers_temp to papers

echo "Not rsyncing"
#rsync -a --verbose --exclude='*.tar' --exclude='*.zip' --exclude='*.rar' * ../papers/
