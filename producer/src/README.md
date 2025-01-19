# Producer CLI

This CLI provides a set of utility to feed data to the `merger` service.

Thanks to this cli you can simulate the following events : 

## 0. Run the producer

To run the producer, you need to run the following command :

```rust
cargo run -- -d 30 run
```

The producer will run for 30 seconds. You can change the duration with the `-d` option.


## 1. Drivers

To simulate driver creations run the following command : 

```rust
cargo run -- driver
```

## 2. Trucks

To simulate truck creations run the following command : 

```rust
cargo run -- truck
```

## 3. Time registration

The time registration events needs the creation of trucs and drivers so it will create
`n` drivers then `n` trucks and finally `n` time registration events. So you
don't need to manage the truck and drivers creation by yourself.
Run the following command :

```rust
cargo run -- time-registration
```

## 4. Position

The position registrations events **calls alll the events** so if you need an end to end
data provider for your merger please use this command. To do so, call :

```rust
cargo run -- position
```

Enjoy :)
