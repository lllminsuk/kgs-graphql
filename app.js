const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const { OGM } = require("@neo4j/graphql-ogm");
const neo4j = require("neo4j-driver");

const AURA_ENDPOINT = "neo4j+s://1eed4b64.databases.neo4j.io";
const USERNAME = "neo4j";
const PASSWORD = "1LlRRhK6aoBn-ofn12xZXJ0-hWRAoGamSPR2bB1ykbg";

const NODE_LIMIT = 49998
const RELATION_LIMIT = 174999

const driver = neo4j.driver(
  AURA_ENDPOINT,
  neo4j.auth.basic(USERNAME, PASSWORD)
);

const typeDefs = `
    type MyNode {
      type: String!
      value: String!
      relOut: [MyNode!]! @relationship(type: "REL", properties: "MyRel", direction: OUT)
      relIn: [MyNode!]! @relationship(type: "REL", properties: "MyRel", direction: IN)
    }
    
    interface MyRel @relationshipProperties {
      name: [String!]
    }

    type Mutation {
      insertTwoNodes(
        E1_value: String!
        E1_type: String!
        E2_value: String!
        E2_type: String!
        REL: String!
      ): Boolean!
    }
`;

const ogm = new OGM({ typeDefs, driver });
const MyNode = ogm.model("MyNode");

const resolvers = {
  Mutation: {
    insertTwoNodes: async (
      _,
      { E1_value, E1_type, E2_value, E2_type, REL },
      ___
    ) => {
      const selectionSet = `
          {
              relOut {
                type
              }
          }
      `;
      let nodeNum = await (await MyNode.find()).length
      let relationNum = await MyNode.find({ selectionSet }).then((nodes)=>{
        let num = 0
        nodes.map((items)=>{
          num += items.relOut.length
        })
        return num
      })
      console.log(relationNum)

      if (nodeNum>=NODE_LIMIT) {
        //노드 2개 삭제 후 관련 RELATION 모두 삭제
      }
      if (relationNum>=RELATION_LIMIT) {
        //1개 RELATION 삭제 후 노드 남겨둠
      }

      //Node 존재여부 체크필요
      let isNode1Exist = await MyNode.find({
        where: { value: E1_value, type: E1_type },
      });
      let isNode2Exist = await MyNode.find({
        where: { value: E2_value, type: E2_type },
      });
      isNode1Exist = isNode1Exist.length >= 1;
      isNode2Exist = isNode2Exist.length >= 1;

      //둘다 존재하지 않을때
      if (!isNode1Exist && !isNode2Exist) {
        MyNode.create({
          input: [
            {
              value: E1_value,
              type: E1_type,
              relOut: {
                create: {
                  node: { value: E2_value, type: E2_type },
                  edge: {
                    name: REL,
                  },
                },
              },
            },
          ],
        });
      }
      //node2만 존재할때
      else if (!isNode1Exist && isNode2Exist) {
        MyNode.create({
          input: [
            {
              value: E1_value,
              type: E1_type,
              relOut: {
                connect: {
                  where:{
                    node: { value: E2_value, type: E2_type },
                  },
                  edge: {
                    name: REL,
                  },
                },
              },
            },
          ],
        });
      }
      //node1만 존재할때
      else if (isNode1Exist && !isNode2Exist) {
        MyNode.create({
          input: [
            {
              value: E2_value,
              type: E2_type,
              relIn: {
                connect: {
                  where:{
                    node: { value: E1_value, type: E1_type },
                  },
                  edge: {
                    name: REL,
                  },
                },
              },
            },
          ],
        });
      }
      //둘다 존재할때
      else {
        MyNode.update({
          where: { 
            value: E1_value,
            type: E1_type
          },
          update: { 
            relOut: {
              connect: {
                where:{
                  node: { value: E2_value, type: E2_type },
                },
                edge: {
                  name: REL,
                },
              },
            },
          },
        })
      }
      return true;
    },
  },
};

const neo4jSchema = new Neo4jGraphQL({ typeDefs, driver, resolvers });

// Generate schema
Promise.all([neo4jSchema.getSchema(), ogm.init()]).then(([schema]) => {
  const server = new ApolloServer({
    schema,
    context: ({ req }) => ({ req }),
  });

  server.listen().then(({ url }) => {
    console.log(`🚀 Server ready at ${url}`);
  });
});