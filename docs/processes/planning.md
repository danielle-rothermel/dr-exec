# Contract-led planning

This process turns a design proposal into reviewable terminology and contract
changes without treating the proposal as repository-wide authority before it is
accepted. The repository's standing vocabulary and contracts remain in
`.defs/terms.toml` and `.defs/contracts.toml`. An active plan keeps its proposed
additions, refinements, contradictions, and omissions beside the design while
those clauses remain under review.

## 1. Freeze the comparison baseline

Before classifying a plan, record the source contract file, its exact commit,
and the comparison date in every plan contract file:

```toml
[baseline]
source = ".defs/contracts.toml"
commit = "<full commit SHA>"
date = "<YYYY-MM-DD>"
```

Copy every parent used by an extension or contradiction into that entry in
full, including its title, statement, rationale, date, and optional check, and
prefix the copied fields with `parent_`. Unaddressed entries are also full
copies. Intentional exclusions reference parent titles only. Each copy or
reference is bound to the frozen baseline rather than a live include: planning
and repository-wide contracts may then evolve independently and their drift is
visible rather than silently synchronized.

## 2. Extract and normalize terminology first

Read the plan for words or phrases that carry a specialized, misuse-detectable
meaning. Compare each candidate with `.defs/terms.toml` before analyzing
contracts:

- Broaden or consolidate an existing core term when the plan clarifies a
  repository-wide concept; then standardize the plan on that term.
- Add a missing core term to `.defs/terms.toml` when the concept already belongs
  to the repository's execution model rather than to this proposal.
- Add a plan-specific term to `docs/<plan>/terms.toml` when the plan introduces
  the concept, protocol, policy, or named surface.
- Do not define incidental implementation names whose ordinary meaning is
  sufficient.

A term says what a concept means. A contract states an independently violable
rule about behavior, ownership, scope, or evolution. A definition can support a
contract but does not replace one.

## 3. Mine contract clauses

Read the full design, including scope, deferred work, implementation boundaries,
testing ownership, packaging, and decisions to revisit. Extract each
independently violable statement. Scope exclusions and authority or evolution
rules are contracts when violating them would change what the proposal means.

Classify clauses, not documents or parent contracts. One parent contract may
have compatible refinements, a conflicting proposal clause, and behavior the
plan intentionally excludes, so it may appear in more than one classification.
Keep unrelated clauses separate when they can be accepted, rejected, or
enforced independently.

## 4. Classify the clauses

Create these classification artifacts beside the plan:

- The extensions artifact contains compatible refinements of existing
  repository contracts. It must not contain the conflicting part of a proposal.
- The contradictions artifact contains proposal clauses that cannot coexist
  with their parent contract as written. Each entry explains the exact conflict.
- The new-contracts artifact contains independently violable proposal rules
  that do not directly refine an existing contract.
- The intentional-scope artifact is a compact index of behavior the plan
  deliberately excludes. It references related parent titles but does not copy
  their complete text, so reviewers can confirm the boundary quickly before
  concentrating on in-scope rules.
- The unaddressed-contracts artifact contains exact copies of current parent
  contracts the proposal neither implements, refines, contradicts, nor
  intentionally excludes.

An intentional-scope entry counts as addressing every parent named in
`related_contracts`. If a future plan understands a contract but postpones
deciding its disposition, record that in a separate deferred classification;
do not call it intentionally out of scope or unaddressed.

## 5. State enforcement explicitly

Each proposed clause has a present-tense `statement`, a `rationale`, one or more
closed `enforcement_modes`, and an exact `enforcement` description. Use only:

- `validation_rejection` — invalid input or configuration is rejected.
- `constructed_invariant` — the API or data shape makes the rule true by
  construction.
- `runtime_guarantee` — executor behavior actively maintains the rule.
- `modeled_outcome` — the result or record represents the distinction.
- `scope_exclusion` — the surface is explicitly outside the proposal.
- `governance` — review, release, test ownership, or change protocol maintains
  the rule.

Avoid an unqualified “requires.” Say what is rejected, constructed, performed,
recorded, excluded, or reviewed so enforcement is testable.

Extension entries use this shape:

```toml
[[extensions]]
parent_title = "..."
parent_statement = "..."
parent_rationale = "..."
parent_date = "<YYYY-MM-DD>"
parent_check = "..." # only when the parent has one
title = "..."
statement = "..."
rationale = "..."
enforcement_modes = ["runtime_guarantee"]
enforcement = "..."
```

Contradiction entries use the same shape under `[[contradictions]]` and add a
`conflict` field. New entries use the proposal fields under `[[contracts]]`.
Intentionally-out-of-scope entries use the following compact shape:

```toml
[[exclusions]]
title = "..."
statement = "..."
rationale = "..."
related_contracts = ["<exact parent title>"]
enforcement_modes = ["scope_exclusion"]
enforcement = "..."
```

`related_contracts` may name more than one parent when one indivisible exclusion
crosses their boundaries. Unaddressed entries use exact parent records under
`[[contracts]]`, with their original field names because there is no proposal
clause to distinguish.

## 6. Verify coverage and review in risk order

Parse every file as TOML and verify that baseline metadata agrees. Compare
copied parent fields with the frozen source revision, check that every current
parent title appears in an extension, contradiction, intentional-scope
reference, or unaddressed entry, and check that proposed titles are unique
within their class. Review the results in this order:

1. Contradictions, because they require changing either the standing rule or
   the proposal.
2. Intentionally out-of-scope behavior, as a quick confirmation of the review
   boundary.
3. New contracts, because they expand the repository's authority surface.
4. Extensions, because they make existing rules concrete.
5. Unaddressed contracts, because silence may reveal a design gap.

Coverage is a review aid, not proof that the classification is correct. Review
must still challenge whether each clause is independently violable, assigned to
the right parent, and backed by the stated enforcement.

## 7. Consolidate, qualify, and activate accepted contracts

When a plan is accepted but its implementation is not yet qualified, consolidate
its surviving clauses into `docs/<plan>/contracts.toml`. Merge compatible
refinements, resolve contradictions, add genuinely new rules, and preserve one
coherent present-tense contract per behavior. Confirm intentional exclusions as
part of acceptance and retain the ones that define the plan's standing scope.
Delete the superseded classification files after verifying the consolidated set.

The planned contract file declares its lifecycle explicitly:

```toml
[contract_set]
schema_version = 1
status = "planned"
activation = "<qualification required before replacement>"
replacement_target = ".defs/contracts.toml"
```

Each `[[contracts]]` entry has a stable `key`, unique `title`, `statement`,
`rationale`, `date`, `enforcement_modes`, and concrete `enforcement`. It contains
no frozen-parent or classification history. The repository-wide
`.defs/contracts.toml` remains active while the plan is implemented and
qualified.

After the complete plan passes its stated qualification, replace
`.defs/contracts.toml` with the qualified set and update `.defs/terms.toml` for
vocabulary that has become core. History belongs in dated changelog entries,
not standing documentation.
