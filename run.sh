#! /bin/bash

#cp bin/edu/umich/clarity/sim.jar ./

#n_bg=(1 2 4 6 8)

load=$1

n_bg=(8)

if [ $load == "high" ]; then
    n_bg=(8)
    echo "n_bg is "+$n_bg
fi
if [ $load == "med" ]; then
    n_bg=(6)
    echo "n_bg is "+$n_bg
fi
if [ $load == "low" ]; then
    n_bg=(4)
    echo "n_bg is "+$n_bg
fi
if [ $load == "all" ]; then
    n_bg=(4 6 8)
    echo "n_bg is "+$n_bg
fi

service=(face dig imc pos ner)
#bg_service1=(face dig imc)
bg_service1=(dig imc)
bg_service2=(pos ner stemmer)

###########PCI-e intensive + PCI-e intensive
#for tg in ${bg_service2[@]}; do
#    for bg in ${bg_service2[@]}; do
#        for b in ${n_bg[@]}; do
#            echo "end_to_end" > simulated/sim-${tg}-${n_bg}-${bg}.csv
#            for((i=0;i<50;i++))
#            do
#                java -jar sim.jar ${tg} ${bg} 1 ${b} 200 10000
#            done
#        done
#    done
#done

###########GPU intensive + GPU intensive
for tg in ${bg_service1[@]}; do
    for bg in ${bg_service1[@]}; do
        for b in ${n_bg[@]}; do
            echo "end_to_end" > simulated/sim-${tg}-${n_bg}-${bg}.csv
            for((i=0;i<50;i++))
            do
                java -jar sim.jar ${tg} ${bg} 1 ${b} 200 2000
            done
        done
    done
done

##########GPU intensive + PCI-e intensive
#for tg in ${bg_service1[@]}; do
#    for bg in ${bg_service2[@]}; do
#        for b in ${n_bg[@]}; do
#            echo "end_to_end" > simulated/sim-${tg}-${n_bg}-${bg}.csv
#            for((i=0;i<50;i++))
#            do
#                java -jar sim.jar ${tg} ${bg} 1 ${b} 200 10000
#            done
#        done
#    done
#done

##########PCI-e intensive + GPU intensive
#for tg in ${bg_service2[@]}; do
#    for bg in ${bg_service1[@]}; do
#        for b in ${n_bg[@]}; do
#            echo "end_to_end" > simulated/sim-${tg}-${n_bg}-${bg}.csv
#            for((i=0;i<50;i++))
#            do
#                java -jar sim.jar ${tg} ${bg} 1 ${b} 200 2000
#            done
#        done
#    done
#done

