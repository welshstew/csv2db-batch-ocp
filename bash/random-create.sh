#!/bin/bash

array[0]="James"
array[1]="John"
array[2]="Tom"
array[3]="Gareth"
array[4]="Kathy"
array[5]="Sian"
array[6]="Rhian"
array[7]="Nicola"
array[8]="Geoff"
array[9]="Ben"
array[10]="Tracy"
array[11]="Matthew"
array[12]="David"
array[13]="Christina"
array[14]="Olive"
array[15]="Sarah"
array[16]="Stephanie"
array[17]="Steven"
array[18]="Mohammad"
array[19]="Faisal"
array[20]="Bill"

surname[0]="Jones"
surname[1]="Williams"
surname[2]="Winchester"
surname[3]="Smith"
surname[4]="Fletcher"
surname[5]="Charles"
surname[6]="Thomas"
surname[7]="Healy"
surname[8]="Farr"
surname[9]="Holmes"
surname[10]="Alun"
surname[11]="Taylor"
surname[12]="Brown"
surname[13]="Davies"
surname[14]="Roberts"
surname[15]="Robinson"
surname[16]="Green"
surname[17]="Lewis"
surname[18]="Martin"
surname[19]="Lee"
surname[20]="Morgan"
size=${#array[@]}
for i in {1..10};
do
index1=$(($RANDOM % $size))
index2=$(($RANDOM % $size))
echo "${array[$index1]},${surname[$index2]}" >> $FILENAME
done;