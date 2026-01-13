---
name: git-fixup-push
description: Combines all git commits for a change into a single commit and pushes it to gerrit. Use when you push to gerrit.
license: Apache-2.0
metadata:
  version: "1.0"
---

## Fixup Git Commits and Push to Gerrit

Use git-fixup-push when you have multiple commits for a change and want to combine them
into a single commit before pushing to gerrit. Changes to Impala are managed in gerrit,
and each change is represented as a single git commit. Use git-fixup-push to meld multiple
commits into a single commit that can be pushed to gerrit.

## Available Scripts

- `scripts/fixup.sh` - Script to combine multiple commits into a single commit.
  Run with the `--help` argument to see usage instructions. Run with the `--dry-run`
  argument to see the exact command that would be executed without actually running it.
  The script selects commits from the most recent `IMPALA-<id>` commit through `HEAD`.
  If that matching commit is already `HEAD`, the script exits without squashing.

## Workflow

- [ ] Step 1: Ensure there is a git remote named `asf-gerrit`. Validate the effective
              fetch and push URLs using `git remote get-url asf-gerrit` and
              `git remote get-url --push asf-gerrit`. Step 1 succeeds when both effective
              URLs match `ssh://<username>@gerrit.cloudera.org:29418/Impala-ASF`, where
              `<username>` is a gerrit username. If this remote does not exist, ask for
              the correct `<username>` to use and add it with the command:
              `git remote add asf-gerrit ssh://<username>@gerrit.cloudera.org:29418/Impala-ASF`.
              If the remote exists but either effective URL does not match, update the
              remote URL configuration and re-validate. Do not run `git remote -v` as
              part of this step since it may contain sensitive information. If this step
              fails, immediately abort the workflow and do not proceed to the next step.
- [ ] Step 2: Check if there are any git changes staged to be committed. If there are,
              commit those changes with a commit message of "commit staged files" and
              proceed to the next step. If there are no staged changes, proceed to the
              next step.
- [ ] Step 3: Check if there are any git changes not staged to be committed. If there are
              unstaged changes, ask the user how to proceed with the unstaged changes. The
              changes can either be staged with `git add` or ignored by stashing with
              `git stash -m "stashed by git-fixup-push skill"`. Ignore all untracked
              files. If the user input is invalid, immediately abort the workflow and do
              not proceed to the next step.
- [ ] Step 4: Check if there are any git files staged to be committed. If there are staged
              files, create a git commit with those files and a commit message of
              "WIP: commit staged files" and proceed to the next step. If there are no
              staged files, proceed to the next step without creating a commit.
- [ ] Step 5: Combine commits using `scripts/fixup.sh`. This script will create a single
              git commit with all commits for the current change fixed up into one. If
              this step fails, immediately abort the workflow and do not proceed to the
              next step.
- [ ] Step 6: Perform code critique on the combined commit. From the folder specified by
              the `${IMPALA_HOME}` environment variable, run the code critique tool using
              the exact command `./bin/jenkins/critique-gerrit-review.py --dryrun`. If the
              command returns a non-zero exit code, that indicates the critique failed. If
              the critique fails, each file with an issue will be listed in the output
              along with the specific issues found in that file. Correct the issues in
              each file, create a new git commit with the corrections and a commit message
              of "fixed critique issues", and start this workflow again from the beginning
              at Step 1. If this step fails a second time, immediately abort the workflow
              and do not proceed to the next step.
- [ ] Step 7: Ensure the most recent git commit contains a line beginning with
              `Assisted-by:`. If this line is not present, ask the user if it should be
              added. If the user confirms it should be added, add a line to the end of the
              most recent commit message in the format `Assisted-by: <model> (<agent-name>)`
              where `<model>` is the name of the current model and `<agent-name>` is the
              name of the agent using this skill. If the user declines to add this line,
              proceed without adding it. If the user input is invalid, immediately abort
              the workflow and do not proceed to the next step.
- [ ] Step 8: Push the commit to gerrit by running the exact command without any
              modifications: `git push asf-gerrit HEAD:refs/drafts/master`.

## Gotchas

- Gerrit requires `Change-Id:` to be in the final footer paragraph of the commit message.
  Keep all trailer lines contiguous in that last paragraph (for example, `Change-Id:` and
  `Assisted-by:` should be adjacent, with no blank line between them).
- Git may not have `remote.<name>.pushurl` explicitly set. In that case,
  `git remote get-url --push <name>` may still resolve via `remote.<name>.url`.
