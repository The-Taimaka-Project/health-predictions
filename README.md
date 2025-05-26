# ETL and Inference Code for Predictive Models of Health Outcomes

Authors:
- Brianna Eales
- Brian Chaplin
- Hunter Merrill

## Description

This codebase contains functionality for extracting, transforming, and loading (ETL) patient data to prepare it as input for downstream predictive models of patient outcomes, as well as running inference on this data using those models. DigitalOcean's [Functions](https://docs.digitalocean.com/products/functions/) are used to schedule ETL and inference jobs.

## Structure

- `packages/etl`: ETL scripts for preparing inference data.
- `packages/inference`: Scripts for running inference on the prepared data.
- `project.yml`: The configuration file defining the DigitalOcean Functions.

## Cost

DigitalOcean Functions pricing is described [here](https://www.digitalocean.com/pricing/functions). In short, the cost is RAM (GiB) times runtime (seconds), minus a 90k GiB $\cdot$ s allowance per month, times $0.0000185. I don't know yet how long the code runs or how much RAM it uses, but the table below shows some expected cost based on some assumptions.

| Runs per month | Assumed RAM used | Assumed runtime | **Cost per month** |
| -------------: | ---------------: | --------------: | -----------------: |
|             30 |              1GB |          30 min |          **$0.00** |
|             30 |              8GB |          15 min |          **$2.31** |
|             30 |              4GB |          30 min |          **$2.31** |
|             30 |              8GB |          30 min |          **$6.33** |
|             30 |             16GB |          1 hour |         **$30.30** |

## Potential Issues

DigitalOcean Functions do not have an R runtime. We may need to migrate Brianna's code to Python.

## Next Steps

- Assess whether Brianna's code can be migrated to Python
- Ensure the ETL Function can authenticate to and access the Taimaka Postgres database

## For Developers

### Description

This repo is modeled after DigitalOcean's [Functions Quick Start](https://docs.digitalocean.com/products/functions/getting-started/quickstart/) page. A Function is a block of code that runs on demand without the need to manage any infrastructure (i.e., it is "serverless"). I chose to use Functions because they are serverless and can be scheduled.

### Installing `doctl`

(I'm learning DigitalOcean as I go, so currently I'm just documenting my progress, and I'll clean this up later.)

I'll document how I set this up from my mac. First I installed `doctl`:
```bash
brew install doctl
```

In my DigitalOcean console in the browser, I created an API token, then I ran the following code in my terminal to authenticate my local machine to my DigitalOcean account (TODO: need to set up a Taimaka context as well, see commented code below):
```bash
doctl auth init  # set up a base context. I pasted my token when prompted.

## once I have Taimaka credentials for DigitalOcean, I'll run this too:
# doctl auth init --context taimaka
```

I also installed serverless functions:
```bash
doctl serverless install
```

There are more details on installation and troubleshooting on DigitalOcean's [installation page](https://docs.digitalocean.com/reference/doctl/how-to/install/).

### Setup and deployment

I created a namespace for testing (TODO: create a namespace within the Taimaka DigitalOcean account)
```bash
doctl serverless namespaces create --label hunter-taimaka-example --region nyc1
```

And I deployed the two "hello, world" functions in this repo to that namespace:
```bash
# run this from the root directory that contains `project.yml`:
doctl serverless deploy .
```

This gave me the following output (which I edited to remove the UUIDs identifying the namespace and host):
```
Deploying '/Users/hunter.merrill/dev/repos/health-predictions'
  to namespace '<uuid redacted>'
  on host 'https://<host redacted>.doserverless.co'
Deployment status recorded in '.deployed'

Deployed functions ('doctl sls fn get <funcName> --url' for URL):
  - etl/hello
  - inference/hello
Deployed triggers:
  - trigger-etl-every-minute
```

In my DigitalOcean dashboard in the browser, I saw that the `etl/hello` function was indeed running every minute. So I then undeployed both functions:
```bash
doctl serverless undeploy etl/hello
doctl serverless undeploy inference/hello
```