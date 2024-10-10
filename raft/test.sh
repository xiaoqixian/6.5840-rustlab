# !/bin/sh
# Date:   Thu Oct 10 09:35:46 2024
# Mail:   lunar_ubuntu@qq.com
# Author: https://github.com/xiaoqixian

while [[ $# -gt 0 ]]; do
    case $1 in
        --features) features="--features=$2"; shift 2;;
        --no-default-feature) no_default_features="--no-default-features"; shift 1;;
        *) test_unit="$test_unit $1"; shift 1;;
    esac
done
test_unit=`echo $test_unit | sed -e 's/^[ \t]*//; s/[ \t]*$//'`

test_one() {
    RUSTFLAGS="-Awarnings" cargo test $1 $no_default_features $features -- --nocapture
}

test_3b() {
    tests=(test3b_basic_agree test3b_rpc_byte test3b_follower_failure test3b_leader_failure test3b_fail_agree test3b_fail_no_agree test3b_concurrent_starts test3b_rejoin test3b_backup test3b_count)
    for t in ${tests[@]}; do
        echo $t
        test_one $t
        if [[ $? -ne 0 ]]; then
            exit 1
        fi
    done
}

case "$test_unit" in
    3B) test_3b;;
    *) for t in $test_unit; do
        test_one $t
    done;;
esac
