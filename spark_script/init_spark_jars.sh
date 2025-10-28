#!/bin/bash
# ==========================================================
# setup_spark_jars.sh
# Downloads required Hadoop + AWS SDK JARs for Spark
# ==========================================================

# Exit immediately if a command fails
set -e

# Define directory for jars
JARS_DIR="/app/spark_jars"

# Create directory if it doesn't exist
mkdir -p "$JARS_DIR"
cd "$JARS_DIR"

echo "📦 Downloading required Spark JARs into $(pwd)..."

# Define URLs
HADOOP_AWS_JAR="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar"
AWS_SDK_BUNDLE_JAR="https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.23.19/bundle-2.23.19.jar"
AWS_JAVA_SDK_BUNDLE_JAR="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.414/aws-java-sdk-bundle-1.12.414.jar"

# Download if not already present
if [ ! -f "hadoop-aws-3.4.0.jar" ]; then
  wget -q "$HADOOP_AWS_JAR"
  echo "✅ hadoop-aws-3.4.0.jar downloaded."
else
  echo "⏩ hadoop-aws-3.4.0.jar already exists, skipping."
fi

if [ ! -f "bundle-2.23.19.jar" ]; then
  wget -q "$AWS_SDK_BUNDLE_JAR"
  echo "✅ bundle-2.23.19.jar downloaded."
else
  echo "⏩ bundle-2.23.19.jar already exists, skipping."
fi

if [ ! -f "aws-java-sdk-bundle-1.12.414.jar" ]; then
  wget -q "$AWS_JAVA_SDK_BUNDLE_JAR"
  echo "✅ aws-java-sdk-bundle-1.12.414.jar downloaded."
else
  echo "⏩ aws-java-sdk-bundle-1.12.414.jar already exists, skipping."
fi

echo "🎉 All Spark JARs are ready in $JARS_DIR/"
