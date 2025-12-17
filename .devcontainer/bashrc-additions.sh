# Start Impala devcontainer customizations.
export IMPALA_HOME="${HOME}/impala"
export IMPALA_LINKER="mold"
export LD_LIBRARY_PATH="/usr/lib/$(uname -p)-linux-gnu"
export LC_ALL="${LOCALE}"

get_current_branch() {
  local ref
  ref=$(git symbolic-ref --short HEAD 2>/dev/null)
  [ -n "${ref}" ] && echo " [${ref}]"
}
export PS1="\\u:\\w\$(get_current_branch)\\\$ "

setup-asf-gerrit-remote() {
  GIT_CFG_USER="$(git config user.name)"
  read -rp "Enter Github.com Username, press return to use default of '${GIT_CFG_USER}'> " GIT_USER
  if [[ -z "${GIT_USER}" ]]; then
    GIT_USER="${GIT_CFG_USER}"
  fi

  pushd "${IMPALA_HOME}"
  git remote add asf-gerrit "ssh://${GIT_USER}@gerrit.cloudera.org:29418/Impala-ASF" 2>/dev/null
  git remote set-url asf-gerrit "ssh://${GIT_USER}@gerrit.cloudera.org:29418/Impala-ASF"
  echo
  echo "asf-gerrit remote:"
  git remote -v | grep asf-gerrit
  echo
  popd
  echo
  echo "Add this SSH public key to Gerrit: "
  cat "${HOME}/.ssh/id_rsa.pub"
  echo
}

omake ()
{
  BASENAME="${1%.*}"
  BASENAME=${BASENAME##*/}
  MAKEFILE=$(grep -l "$BASENAME.cc.o:" $IMPALA_HOME/be/src/*/CMakeFiles/*.dir/build.make)
  MAKEFILE=${MAKEFILE#$IMPALA_HOME/}
  make DEBUG_NOOPT=1 -s -f $MAKEFILE ${MAKEFILE%/*}/$BASENAME.cc.o
}

mvn-debug-front-end() {
  local target_test=${1}
  if [[ -z ${target_test} ]]; then
    echo "Error: undefined target test"
  else
    mvn \
      -Dmaven.surefire.debug="-Xdebug \
      -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000 \
      -Xnoagent \
      -Djava.compiler=NONE" \
      -fae \
      -Dtest="${target_test}" \
      test
  fi
}

mvn-run-front-end() {
  local target_test=${1}
  if [[ -z ${target_test} ]]; then
    echo "Error: undefined target test"
  else
     mvn test -Dtest="${target_test}"
  fi
 }

resolve-minidump() {
  local log_dir="${1}"
  local minidump_file=$(grep "Wrote minidump to " "${log_dir}/impalad.ERROR" | rev | cut -d" " -f1 | rev)

  echo "Resolving minidump: ${minidump_file}"
  resolve_minidumps.py --minidump_file "${minidump_file}" --output_file /tmp/resolve.txt
  cat /tmp/resolve.txt | head -n 100
}

alias ga="git add"
alias gb="git branch"
alias gba="git branch -a"
alias gc="git commit -v"
alias gd="git diff"
alias grv="git remote -v"
alias gs="git status"
alias critique='pushd "${IMPALA_HOME}" && ./bin/jenkins/critique-gerrit-review.py --dryrun; popd'
alias restart-minicluster='pushd "${IMPALA_HOME}" && ./testdata/bin/run-all.sh; popd'
alias push-asf-gerrit-draft='git push asf-gerrit HEAD:refs/drafts/master'"
alias push-asf-gerrit-publish='git push asf-gerrit HEAD:refs/for/master'"

unset IMPALA_TOOLCHAIN_COMMIT_HASH
. "${IMPALA_HOME}/bin/impala-config.sh"
. "${IMPALA_HOME}/bin/set-classpath.sh"
cd "${IMPALA_HOME}"
# End Impala devcontainer customizations.
