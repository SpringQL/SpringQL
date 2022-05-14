function get_crate_version() {
    curl -L https://crates.io/api/v1/crates/$1/versions | jq -r '.versions[] | select( .yanked == false) | .num' | sort --version-sort | tail -n 1
}

export -f get_crate_version