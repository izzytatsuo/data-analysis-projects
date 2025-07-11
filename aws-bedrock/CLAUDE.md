# Redshift Querying via Bedrock Knowledge Base

## Query Method

To query Redshift data using the AWS Bedrock knowledge base:

1. Use the `aws bedrock-agent-runtime retrieve` command
2. Format queries as JSON files with the `text` field containing the natural language question
3. Create the JSON file with proper escaping: `echo '{"text": "your query here"}' > query.json`
4. Submit the query using: `aws bedrock-agent-runtime retrieve --knowledge-base-id RD2ZPKUAUT --retrieval-query file://query.json`
5. The knowledge base ID for Redshift is `RD2ZPKUAUT`

## Example Query

```bash
# Create the query file
echo '{"text": "show all tables in the database"}' > query.json

# Submit the query to Bedrock
aws bedrock-agent-runtime retrieve --knowledge-base-id RD2ZPKUAUT --retrieval-query file://query.json
```

This approach allows for natural language queries to be translated into SQL and executed against the Redshift database.