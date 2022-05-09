const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const { OGM } = require("@neo4j/graphql-ogm");
const neo4j = require("neo4j-driver");

const AURA_ENDPOINT = "neo4j+s://1eed4b64.databases.neo4j.io";
const USERNAME = "neo4j";
const PASSWORD = "1LlRRhK6aoBn-ofn12xZXJ0-hWRAoGamSPR2bB1ykbg";

const NODE_LIMIT = 100//50000
const RELATION_LIMIT = 5//175000

const driver = neo4j.driver(
  AURA_ENDPOINT,
  neo4j.auth.basic(USERNAME, PASSWORD)
);

const typeDefs = `
    type MyNode {
      type: String!
      value: String!
      createdAt: DateTime! @timestamp(operations: [CREATE, UPDATE])
      relOut: [MyNode!]! @relationship(type: "REL", properties: "MyRel", direction: OUT)
      relIn: [MyNode!]! @relationship(type: "REL", properties: "MyRel", direction: IN)
    }
    interface MyRel @relationshipProperties {
      name: [String!]
    }
    type Query {
      search(
        limit: Int!
        sentence: String!
      ): [MyNode]!
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
    type Info {
      id: Int!
      countNode: Int!
      @cypher(
        statement: """
        MATCH (n)
        RETURN count(*)
        """
      )
      countRelation: Int!
      @cypher(
            statement: """
            MATCH (n)-[r]->(x)
            RETURN count(r)
            """
        )
      deleteTwoNodes: Boolean
      @cypher(
            statement: """
            MATCH (N) 
            WHERE N.value IS NOT NULL 
            WITH N 
            ORDER BY N.createdAt 
            LIMIT 2 
            DETACH DELETE N
            """
      )
    }
`;

const ogm = new OGM({ typeDefs, driver });
const MyNode = ogm.model("MyNode");
const Info = ogm.model("Info")

const resolvers = {
  Query: {
    search: async (
      _,
      { limit, sentence },
      ___
    ) => {
      const selectionSet = `
        {
          value
          relOut {
            value
          }
          relIn {
            value
          }
        }
      `;
      var returnList = []
      var keyword = sentence.split(" ")
      await Promise.allSettled(keyword.map(async (word) => {
        var tempNode = await MyNode.find({ selectionSet: selectionSet,
          where: {
            value: word,
          }
        })
        returnList.push(JSON.parse(JSON.stringify(tempNode[0])))
      }));
      console.log(returnList)
      return returnList
    },
  },
  Mutation: {
    insertTwoNodes: async (
      _,
      { E1_value, E1_type, E2_value, E2_type, REL },
      ___
    ) => {
      const selectionSet = `
          {
            countNode
            countRelation
          }
      `;
      let desc
      await Info.find({ selectionSet:selectionSet }).then((nodes)=>{ //관계 개수
        desc = nodes[0];
      })
      
      let nodeNum = desc.countNode //노드 개수
      let relationNum = desc.countRelation
      console.log(nodeNum, relationNum)

      //Node 존재여부 체크필요
      let isNode1Exist = await MyNode.find({
        where: { value: E1_value, type: E1_type },
      });
      let isNode2Exist = await MyNode.find({
        where: { value: E2_value, type: E2_type },
      });
      isNode1Exist = isNode1Exist.length >= 1;
      isNode2Exist = isNode2Exist.length >= 1;

      if (!isNode1Exist && !isNode2Exist && nodeNum+2 > NODE_LIMIT) {
        console.log("deleteTwoNode")
        //노드 2개 삭제 후 관련 RELATION 모두 삭제
        let findDeleteNode = await MyNode.find({
          options: {
            sort: [
              {
                createdAt: "ASC"
              }
            ],
            limit: 2
          }
        })
        findDeleteNode.map(async(node) => {
          await MyNode.delete({
            where: {
              type: node.type,
              value: node.value,
            },
          })
        })
      }
      else if (((!isNode1Exist&&isNode2Exist) || (isNode1Exist&&!isNode2Exist)) && nodeNum+1 > NODE_LIMIT) {
        console.log("deleteOneNode")
        //노드 2개 삭제 후 관련 RELATION 모두 삭제
        let findDeleteNode = await MyNode.find({
          options: {
            sort: [
              {
                createdAt: "ASC"
              }
            ],
            limit: 1
          }
        })
        findDeleteNode.map(async(node) => {
          await MyNode.delete({
            where: {
              type: node.type,
              value: node.value,
            },
          })
        })
      }
      
        let relExist = await MyNode.find({
          where: {
            value: E1_value,
            type: E1_type,
            relOutConnection_SINGLE: {
              node: {
                value: E2_value,
                type: E2_type
              },
            }
          }
        })
        relExist = relExist.length > 0

      if (!(isNode1Exist && isNode2Exist && relExist) && relationNum+1 > RELATION_LIMIT) {
        const selectionSet2 = `
          {
            type
            value
            relOut {
              type
              value
            }
          }
        `;

        let a = await MyNode.find({ selectionSet:selectionSet2, 
          where: {
            relOutAggregate: {
              count_GT: 0
            }
          },
          options: {
            sort: [
              {
                createdAt: "ASC"
              }
            ],
            limit: 1
          }
        })
        let nodeType1 = a[0].type
        let nodeValue1 = a[0].value
        let nodeType2 = a[0].relOut[a[0].relOut.length-1].type
        let nodeValue2 = a[0].relOut[a[0].relOut.length-1].value 
        console.log(nodeType1, nodeValue1, nodeType2, nodeValue2)
        await MyNode.update({
          where: {
            type: nodeType1,
            value: nodeValue1
          },
          update: {
            relOut: [
              {
                disconnect: [
                  {
                    where: {
                      node: {
                        type: nodeType2,
                        value: nodeValue2
                      }
                    }
                  }
                ]
              }
            ]
          }
        })
        console.log(relExist)
      }
      
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
    context: async({ req }) => {
      return { req }
    },
  });
  
  server.listen().then(async({ url }) => {
    console.log(`🚀 Server ready at ${url}`);
    let isInfoExist = await Info.find({
      where: { id: 0 },
    });
    if(isInfoExist.length===0){
      await Info.create({
        input: [
          {
            id: 0
          }
        ]
      })
    }
  });
});
