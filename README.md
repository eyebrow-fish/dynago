# dynago

`dynago` is an extensive wrapper around the [AWS Go Sdk (V2)](https://github.com/aws/aws-sdk-go-v2) — which I find to
not be particularly developer-friendly.

**CURRENTLY BEING REWRITTEN**

# development

The local dynamodb JAR is a must. Without this you cannot run the tests.

**Setup**:
- Download the JAR [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html)
- Unzip to `~/dev/dynamo-local-lib` *(eg. unzip dynamodb_local_latest.zip -d ~/dev/dynamo-local-lib)*
- You're done! Tests **SHOULD** just work.
