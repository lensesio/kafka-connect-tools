# connect-cli bash completion

_connect-cli() {
    if [ "${#COMP_WORDS[@]}" != "2" ]; then
        return
    fi
    subcommand=$(connect-cli --help | grep "Command" | cut -d ' ' -f2 | tr '\n' ' ')
    COMPREPLY=($(compgen -W "$subcommand" "${COMP_WORDS[1]}"))
}

complete -F _connect-cli connect-cli