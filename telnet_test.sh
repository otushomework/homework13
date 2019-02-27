sleep 0.5

SLEEP=0.1
BIGSLEEP=1

#fill table 1
echo "INSERT A 0 lean"
sleep ${SLEEP}
echo "INSERT A 0 understand"
sleep ${SLEEP}
echo "INSERT A 1 sweater"
sleep ${SLEEP}
echo "INSERT A 2 frank"
sleep ${SLEEP}
echo "INSERT A 3 violation"
sleep ${SLEEP}
echo "INSERT A 4 quality"
sleep ${SLEEP}
echo "INSERT A 5 precision"
sleep ${SLEEP}

#fill table 2
echo "INSERT B 3 proposal"
sleep ${SLEEP}
echo "INSERT B 4 example"
sleep ${SLEEP}
echo "INSERT B 5 lake"
sleep ${SLEEP}
echo "INSERT B 6 flour"
sleep ${SLEEP}
echo "INSERT B 7 wonder"
sleep ${SLEEP}
echo "INSERT B 8 selection"
sleep ${SLEEP}

#operations
echo "INTERSECTION"
sleep ${BIGSLEEP}

echo "SYMMETRIC_DIFFERENCE"
sleep ${BIGSLEEP}

#finish
sleep ${SLEEP}
echo "TRUNCATE A"
sleep ${SLEEP}
echo "TRUNCATE B"
sleep ${SLEEP}
