# Lambda Traffic Experiment

Testing the network on AWS Lambda

## Prequisites

You're going to need a rust compiler. [Rustup](https://rustup.rs/) is the
recommended installation tool for the Rust compiler and other rust tools.

You're also going to need an Amazon account with Lambda and S3 access. Once you
have that, you should set the following environmental variables:
   * `AWS_ACCESS_KEY_ID`
   * `AWS_SECRET_ACCESS_KEY`
   * `AWS_REGION` (which should be like "us-west-2")

You're also going to need a public IP. The system will figure out your public IP
on its own, but it does need to be public.

## System Setup

You'll need a rust compiler. Use [rustup](https://rustup.rs/).

You'll need the MUSL target to build for execution on lambda. Run

```
rustup target add x86_64-unknown-linux-musl
```

You may need MUSL on your system. Install it.

## Building and deploying

Build using `cargo build --release`. Deploy the new lambda code by running the
`update-function.py` script from the project root. This must be done after any
change to the remote function.

## Runbook

Start the local program on your machine by running

```
./target/x86_64-unknown-linux-musl/release/local dipair 1 myexperiment
```

(use `-h` to see the options).

Run

```
./analyze myexperiment
```

to download and see the results.


