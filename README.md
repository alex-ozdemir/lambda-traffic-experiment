# Lambda Traffic Experiment

Testing the network on AWS Lambda

## Prequisites

You're going to need a rust compiler. [Rustup](https://rustup.rs/) is the
recommended installation tool for the Rust compiler and other rust tools.

You're also going to need an Amazon account with Lambda and S3 access. Once you
have that, you should set the following environmental variables:
   * `AWS_ACCESS_KEY_ID`
   * `AWS_SECRET_ACCESS_KEY`

You're also going to need a public IP. The system will figure out your public IP
on its own, but it does need to be public.

## Building and deploying

Build using `cargo build --release`. Deploy the new lambda code by running the
`update-function.py` script from the project root.

## Runbook

Start the local program on your machine by running

```
./target/x86_64-unknown-linux-musl/release/local
```

It will run the hard-coded experiment. The results will be in the S3 bucket
"rust-test-2" with a name that includes the experiment ID given to you by the
local program.

