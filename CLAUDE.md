# DGCE Verification and Git Control

Use this file as the minimum operating rule set for verification and git actions in DGCE.

## Verification Checklist

- lifecycle integrity preserved
- Guardrail authority preserved
- Aether control preserved
- validation enforced
- no direct writes introduced
- no scope expansion introduced

## PASS Behavior

Only after the checklist passes:

```bash
git add .
git commit -m "<message>"
git push
```

## FAIL Behavior

- take no git actions
- report only blocking issues

## Explicit Rule

Never commit if architecture is violated.
