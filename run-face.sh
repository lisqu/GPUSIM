#! /bin/bash

#cp bin/edu/umich/clarity/sim.jar ./

#n_bg=(1 2 4 6 8)
n_bg=(8)

service=(face dig imc pos ner)
bg_service1=(dig imc)
bg_service2=(pos ner)

for tg in ${service[@]}; do
    bg=face
    for b in ${n_bg[@]}; do
        echo "end_to_end" > simulated/${tg}-${n_bg}-${bg}-sim.csv
            for((i=0;i<50;i++))
            do
                java -jar sim.jar ${tg} face 1 ${b} 200 1000
            done
    done
done

for bg in ${bg_service1[@]}; do
    tg=face
    for b in ${n_bg[@]}; do
        echo "end_to_end" > simulated/${tg}-${n_bg}-${bg}-sim.csv
            for((i=0;i<50;i++))
            do
                java -jar sim.jar face ${bg} 1 ${b} 200 2000
            done
    done
done

for bg in ${bg_service2[@]}; do
    tg=face
    for b in ${n_bg[@]}; do
        echo "end_to_end" > simulated/${tg}-${n_bg}-${bg}-sim.csv
            for((i=0;i<50;i++))
            do
                java -jar sim.jar face ${bg} 1 ${b} 200 9999
            done
    done
done

#for tg in ${service[@]}; do
#    for bg in ${bg_service1[@]}; do
#        for b in ${n_bg[@]}; do
#            java -jar sim.jar ${tg} ${bg} 1 ${b} 300 3000
#        done
#    done
#done

#for tg in ${service[@]}; do
#    for bg in ${bg_service2[@]}; do
#        for b in ${n_bg[@]}; do
#            java -jar sim.jar ${tg} ${bg} 1 ${b} 300 10000
#        done
#    done
#done
