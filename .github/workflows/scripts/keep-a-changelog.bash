
# edit changelog for release
#
# Usage:
#  cat CHANGELOG.md | bump_changelog [current version] [new version] > edited.md
#
function bump_changelog() {
    CURRENT_VERSION=$1
    NEW_VERSION=$2

    while IFS='' read -r line
    do
        if [ "$line" == "[Unreleased]: https://github.com/SpringQL/SpringQL/compare/${CURRENT_VERSION}...HEAD" ]; then 
            # update line for [Unreleased]
            line="[Unreleased]: https://github.com/SpringQL/SpringQL/compare/${NEW_VERSION}...HEAD"
        fi

        echo "$line"
        if [ "$line" == "## [Unreleased]" ]; then
            # insert line after ## [Unleleased]
            echo # output blank line
            RELEASE_DATE=$(date '+%Y-%m-%d')
            echo "## [${NEW_VERSION}] (${RELEASE_DATE})"
        fi

        if [ "$line" == "[Released]: https://github.com/SpringQL/SpringQL/releases" ]; then
            # insert line after [Released]...
            echo "[${NEW_VERSION}]: https://github.com/SpringQL/SpringQL/compare/${CURRENT_VERSION}...${NEW_VERSION}"
        fi
    done
}

export -f bump_changelog