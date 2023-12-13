#!/bin/bash

HAS_ISSUES=0
FIRST_FILE=1

for file in $(git diff --name-only --staged); do
    FMT_RESULT="$(rustfmt --skip-children --force --write-mode diff $file 2>/dev/null || true)"
    if [ "$FMT_RESULT" != "" ]; then
        if [ $FIRST_FILE -eq 0 ]; then
            echo -n ", "
        fi  
        echo -n "$file"
        HAS_ISSUES=1
        FIRST_FILE=0
    fi
done

if [ $HAS_ISSUES -eq 0 ]; then

    commit_msg_file=$1
    commit_msg=$(cat "$commit_msg_file")

    # Define a regular expression to match repeated mash of a single key
    pattern="^([a-zA-Z0-9])\\1+$"

    # Define words to be avoided in commit messages
    avoid_patterns="^(change|changes|fixes|fix)$"

    if [[ $commit_msg =~ $pattern ]]; then
        echo "Error: Commit message is a repeated mash of a single key."
        exit 1
    elif [[ $commit_msg =~ $avoid_patterns ]]; then
        echo "Error: Commit message is 'change', 'changes', 'fixes', or 'fix', which is not allowed."
        exit 1
    fi

    exit 0
fi

echo ". Your code has formatting issues in files listed above. Format your code with \`make format\` or call rustfmt manually."
exit 1