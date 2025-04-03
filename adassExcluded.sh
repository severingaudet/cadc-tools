#!/bin/bash

for file in [BCFIPT]*
do
	echo $file
	found=$(grep -c $file ../toc.txt)
	if [[ "$found" -eq 0 ]]
	then
		echo "$file not in toc.txt and moving to papers_excluded"
		mv $file ../papers_excluded/
	fi
done
