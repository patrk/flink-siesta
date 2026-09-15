# One Kafka connector line per supported Flink version. Sourced by build.sh and jar.sh.
job_versions() {
  case "$1" in
    1.20) maven=1.20.4; connector=3.4.0-1.20 ;;
    2.0)  maven=2.0.2;  connector=4.0.1-2.0 ;;
    2.1)  maven=2.1.2;  connector=5.0.0-2.1 ;;
    2.2)  maven=2.2.1;  connector=5.0.0-2.2 ;;
    2.3)  maven=2.2.1;  connector=5.0.0-2.2 ;;  # no connector built for 2.3 yet; the 2.2 one runs on the 2.3 image
    *) echo "no connector mapping for Flink $1" >&2; return 1 ;;
  esac
}
