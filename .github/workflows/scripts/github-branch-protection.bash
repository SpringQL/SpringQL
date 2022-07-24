function get_current_branch_protection_setting() {
    gh api --method GET repos/${OWNER}/${REPO}/branches/${BRANCH}/protection | jq '
    {
        required_status_checks: null,
        restrictions: { 
            users: .restrictions.users | [.[].login],
            teams: .restrictions.teams | [.[].slug],
            apps: .restrictions.apps | [.[].slug]
        },
        enforce_admins: .enforce_admins.enabled ,
        required_pull_request_reviews: {
        dismiss_stale_reviews: .required_pull_request_reviews.dismiss_stale_reviews,
        require_code_owner_reviews: .required_pull_request_reviews.require_code_owner_reviews,
        required_approving_review_count: .required_pull_request_reviews.required_approving_review_count
        },
        required_linear_history: .required_linear_history.enabled,
        required_signatures: .required_signatures.enabled,
        allow_force_pushes: .allow_force_pushes.enabled,
        allow_deletions: .allow_deletions.enabled,
        block_reations: .block_creations.enabled,
        required_conversation_resolution: .required_conversation_resolution.enabled
    }'
}

function apply_branch_protection_setting() {
    gh api --method PUT -H "Accept: application/vnd.github+json" --input - repos/${OWNER}/${REPO}/branches/${BRANCH}/protection
}

function enfore_admins_off() {
    get_current_branch_protection_setting | jq '.enforce_admins = false' | apply_branch_protection_setting
}

export -f enfore_admins_off

function enfore_admins_on() {
    get_current_branch_protection_setting | jq '.enforce_admins = true' | apply_branch_protection_setting
}

export -f enfore_admins_on
