# Security policy

## Supported versions

This is a reference implementation maintained on a best-effort basis. Only the latest commit on `main` is supported; there are no release branches.

## Reporting a vulnerability

Please report vulnerabilities privately using GitHub's private vulnerability reporting: open the repository's **Security** tab and choose **Report a vulnerability** ([direct link](https://github.com/chingdrop/medicare-rebuild/security/advisories/new)). Do not open a public issue for a security problem. There is no separate email contact; GitHub private vulnerability reporting is the only channel.

What to expect: an acknowledgement within 7 days and an initial assessment within 14 days. Fixes are made when the maintainer has time; there is no service-level commitment.

## Scope

- **In scope:** the code in this repository, its GitHub Actions workflows, and its dependencies.
- **Data:** this repository contains synthetic demo data only. No real credentials and no real patient data are ever committed. If you believe you have found something that looks like real personal, patient or credential data, report it privately using the process above and describe where it is (file and commit). **Do not attach the data itself.**
- **Documented development settings** are not vulnerabilities on their own: the throwaway SQL Server password used for local and CI containers, and the `TrustServerCertificate=yes` connection option. Both are described in [docs/data-handling.md](docs/data-handling.md). Reports about how they could be misused in a real deployment are still welcome.
