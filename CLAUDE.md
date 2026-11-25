# Instructions for agents

## VERIFIED.md

[VERIFIED.md](VERIFIED.md) lists the properties of redwall that a
mechanical check supports. Readers treat each statement in the file as
true. An agent must keep the file up to date.

Read VERIFIED.md before answering a question about the behaviour of
redwall. Cite an entry by its identifier, such as `R1`.

Update VERIFIED.md in the same commit as a change that does any of the
following:

- adds, removes or weakens a check: a TLA+ model, a TLC config, a
  simulation test, an invariant, a check at the end of a run, a planted
  defect, or a unit test that an entry names
- changes the bound of a check, such as the constants of a config or the
  number of seeds
- makes an entry false, or makes an assumption or a limitation false
- adds a feature that no check covers. Add the feature to "Not verified".
- renames an invariant, a config, a test or a file that the file names
- changes the version of peace, or the options with which redwall uses
  peace. Compare the "Dependencies" section with the `VERIFIED.md` of
  peace again.

Rules for an edit of VERIFIED.md:

- Add an entry only when a check that can fail supports it, and only after
  the check has passed. Do not add an entry from an argument or from a
  reading of the code.
- State the method and the bound of every entry. Take the numbers from the
  check or from its recorded results.
- Do not change the identifier of an entry. Do not use the identifier of a
  removed entry for a new entry.
- VERIFIED.md has no commit in its header. The file describes the version
  of redwall in the same commit. After a run of a check that GitHub
  Actions does not run on each push, update the "Last recorded run" column
  in "Reproduction".
- Keep README.md, docs/verification.md, docs/limitations.md and
  formal/README.md consistent with VERIFIED.md. If two of the files
  disagree, find the correct value in the code or the check, and correct
  every file.

At the end of a change to `src/`, `dst/`, `test/` or `formal/`, compare
the change with VERIFIED.md and state whether the file needed an edit.
