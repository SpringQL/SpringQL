
# function: get_crate_latest_version 
# arg#1: crate name
# returns: latest version number on crates.io
#
#  this calls crates.io REST api via curl, that listing all versions of specified crate.
#  `jq` query filter for not yanked and extract version number.
#  sort semver and takes last one.
function get_crate_latest_version() {
    curl -L https://crates.io/api/v1/crates/$1/versions | jq -r '.versions[] | select( .yanked == false) | .num' | sort --version-sort | tail -n 1
}

export -f get_crate_latest_version

# function: wait_published
# arg#1: crate name
# arg#2: expect version
function wait_published() {
    echo "Waiting $1@$2"
    CURRENT_VERSION=$(get_crate_latest_version $1)
    echo "CURRENT_VERSION=$CURRENT_VERSION"
    while [ $CURRENT_VERSION != "$2" ]
    do
        sleep 5
        CURRENT_VERSION=$(get_crate_latest_version $1)
        echo "CURRENT_VERSION=$CURRENT_VERSION"
    done
}

export -f wait_published

# function: try_dryrun_publish
# arg#1: max retry
# arg#2.. : pass to `cargo publish --dry-run` args
#
# return code:
# 3 : leached max retry 
# other: exit code from cargo publish
function try_publish_dryrun() {
    MAX_RETRY=$1
    NUM_RETRY=0
    echo 'executing first try'
    cargo publish --dry-run ${@:2}
    PUBLISH_DRYRUN_EXITCODE=$?
    while [ ${PUBLISH_DRYRUN_EXITCODE} -eq 101 ];
    do
        NUM_RETRY=`expr ${NUM_RETRY} + 1`
        if [ ${NUM_RETRY} -gt ${MAX_RETRY} ];
        then
          return 3
        fi
        echo 'waiting for retry'
        sleep 30
        echo 'Retrying'
        cargo publish --dry-run ${@:2}
        PUBLISH_DRYRUN_EXITCODE=$?
    done
    retrn ${PUBLISH_DRYRUN_EXITCODE}
}

export -f try_publish_dryrun