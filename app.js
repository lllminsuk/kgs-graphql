const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const neo4j = require("neo4j-driver");

const AURA_ENDPOINT = "neo4j+s://1eed4b64.databases.neo4j.io";
const USERNAME = "neo4j";
const PASSWORD = "1LlRRhK6aoBn-ofn12xZXJ0-hWRAoGamSPR2bB1ykbg";

const resolvers = {
  Query: {
    IS_EXIST: (_, { E1_value, E1_type, E2_value, E2_type, REL }, ___) => {
      console.log(E1_value);
      console.log(E1_type);
      console.log(E2_value);
      console.log(E2_type);
      console.log(REL);
      return "HI";
    },
  },
};

const typeDefs = gql`
  type Query {
    IS_EXIST(
      E1_value: String!
      E1_type: String!
      E2_value: String!
      E2_type: String!
      REL: String!
    ): String!
  }

  type MY_NODE {
    value: String!
    type: String!
    relation: [MY_NODE!]!
      @relationship(type: "REL", properties: "REL", direction: IN)
  }

  interface REL @relationshipProperties {
    name: [String!]
  }
`;

const driver = neo4j.driver(
  AURA_ENDPOINT,
  neo4j.auth.basic(USERNAME, PASSWORD)
);

const neo4jSchema = new Neo4jGraphQL({ typeDefs, driver, resolvers });

// Generate schema
neo4jSchema.getSchema().then((schema) => {
  const server = new ApolloServer({
    schema,
  });

  server.listen({ port: 4000 }).then(({ url }) => {
    console.log(`GraphQL server ready on ${url}`);
  });
});
