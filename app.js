const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const { OGM } = require("@neo4j/graphql-ogm");
const neo4j = require("neo4j-driver");

const AURA_ENDPOINT = "neo4j+s://1eed4b64.databases.neo4j.io";
const USERNAME = "neo4j";
const PASSWORD = "1LlRRhK6aoBn-ofn12xZXJ0-hWRAoGamSPR2bB1ykbg";

const NODE_LIMIT = 50000;
const RELATION_LIMIT = 175000;

const driver = neo4j.driver(
  AURA_ENDPOINT,
  neo4j.auth.basic(USERNAME, PASSWORD)
);

const typeDefs = `
    type D3Node{
      id: String!
      url: String
      type: String!
    }

    type D3Link{
      source: String!
      target: String!
      label: String!
    }

    type D3Return{
      nodes:[D3Node]!
      links:[D3Link]!
    }

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
        words: String!
      ): D3Return!
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
const Info = ogm.model("Info");
const relTag = {
  'no_relation':"관계 없음",
  'org:dissolved':'해당 기관이 해체된 날짜',
  'org:founded':'해당 기관이 설립된 날짜',
  'org:place_of_headquarters':'해당 기관의 본사가 위치한 장소',
  'org:alternate_names':'해당 기관의 대체 이름',
  'org:member_of':'해당 기관의 일원',
  'org:members':'해당 기관에 속한 하위기관',
  'org:political/religious_affiliation':'해당 기관이 소속된 정치/종교 그룹',
  'org:product':'해당 기관에서 생산된 제품',
  'org:founded_by':'해당 기관의 설립자',
  'org:top_members/employees':'해당 기관의 대표 구성원',
  'org:number_of_employees/members':'해당 기관의 소속된 구성원 수',
  'per:date_of_birth':'해당 사람이 태어난 날짜',
  'per:date_of_death':'해당 사람이 죽은 날짜',
  'per:place_of_birth':'해당 사람이 태어난 장소',
  'per:place_of_death':'해당 사람이 죽은 장소',
  'per:place_of_residence':'해당 사람이 거주하는 장소',
  'per:origin':'해당 사람의 국적',
  'per:employee_of':'해당 사람이 근무하는 기관',
  'per:schools_attended':'해당 사람의 출신 학교',
  'per:alternate_names':'해당 사람의 대체 이름',
  'per:parents':'해당 사람의 부모님',
  'per:siblings':'해당 사람의 자식',
  'per:spouse':'해당 사람의 배우자',
  'per:other_family':'해당 사람의 부모, 배우자, 자식을 제외한 나머지 가족',
  'per:colleagues':'해당 사람의 직장동료',
  'per:product':'해당 사람이 생산한 제품',
  'per:religion':'해당 사람의 종교',
  'per:title':'해당 사람의 직위'
}

const resolvers = {
  Query: {
    search: async (_, { limit, words }, ___) => {
      const selectionSet = `
        {
          value
          type
          relOutConnection {
            edges {
              name
            }
          }
          relOut {
            value
            type
          }
          relInConnection {
            edges {
              name
            }
          }
          relIn {
            value
            type
          }
        }
      `;

      var nodes = [];
      var links = [];

      if (words === "") {
        var findedNodes = await MyNode.find({
          selectionSet: selectionSet,
          options: {
            limit: limit
          }
        });
        findedNodes.map((source) => {
          nodes.push({ id: source.value, type: source.type });
          source.relOut.map((target, index) => {
            links.push({
              source: source.value,
              target: target.value,
              label: source.relOutConnection.edges[index].name[0],
            });
          });
        });
      } else {
        var wordsList = words.split(" ");
        for (var i = 0; i < wordsList.length; i++) {
          var findedNodes = await MyNode.find({
            selectionSet: selectionSet,
            where: {
              value: wordsList[i],
            },
          });
          findedNodes.map((source) => {
            //if (nodes.length >= limit) return;
            nodes.push({ id: source.value, type: source.type });
            source.relOut.map((target, index) => {
              //if (nodes.length >= limit) return;
              nodes.push({ id: target.value, type: target.type });
              links.push({
                source: source.value,
                target: target.value,
                label: source.relOutConnection.edges[index].name[0],
              });
            });
            source.relIn.map((target, index) => {
              //if (nodes.length >= limit) return;
              nodes.push({ id: target.value, type: target.type });
              links.push({
                source: target.value,
                target: source.value,
                label: source.relInConnection.edges[index].name[0],})
            });
          });
        }
      }
      // 중복제거
      nodes = nodes.filter((node, idx, arr)=>{
        return arr.findIndex((item) => item.id === node.id && item.id === node.id) === idx
      });
      links = links.filter((link, idx, arr)=>{
        return arr.findIndex((item) => item.source === link.source && item.target === link.target && item.label === link.label) === idx
      });
      for (var i=limit; i<nodes.length; i++) {
        for (var j=0; j<links.length; j++) {
          if (links[j].source===nodes[i].id || links[j].target===nodes[i].id) {
            links.splice(j,1)
            j--
          }
        }
        nodes.splice(i,1)
        i--
      }
      for (var i=0; i<links.length; i++) {
        checkSource = false
        checkTarget = false
        for (var j=0; j<nodes.length; j++) {
          if (links[i].source===nodes[j].id) {
            checkSource = true
          }
          if (links[i].target===nodes[j].id) {
            checkTarget = true
          }
        }
        if (!checkSource || !checkTarget) {
          links.splice(i,1)
          i--
        }
      }
      for (var i=0; i<links.length; i++) {
        links[i].label = relTag[links[i].label]
      }
      console.log({ nodes, links });

      return { nodes, links };
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
      let desc;
      await Info.find({ selectionSet: selectionSet }).then((nodes) => {
        //관계 개수
        desc = nodes[0];
      });

      let nodeNum = desc.countNode; //노드 개수
      let relationNum = desc.countRelation;
      console.log(nodeNum, relationNum);

      //Node 존재여부 체크필요
      let isNode1Exist = await MyNode.find({
        where: { value: E1_value, type: E1_type },
      });
      let isNode2Exist = await MyNode.find({
        where: { value: E2_value, type: E2_type },
      });
      isNode1Exist = isNode1Exist.length >= 1;
      isNode2Exist = isNode2Exist.length >= 1;

      if (!isNode1Exist && !isNode2Exist && nodeNum + 2 > NODE_LIMIT) {
        console.log("deleteTwoNode");
        //노드 2개 삭제 후 관련 RELATION 모두 삭제
        let findDeleteNode = await MyNode.find({
          options: {
            sort: [
              {
                createdAt: "ASC",
              },
            ],
            limit: 2,
          },
        });
        findDeleteNode.map(async (node) => {
          await MyNode.delete({
            where: {
              type: node.type,
              value: node.value,
            },
          });
        });
      } else if (
        ((!isNode1Exist && isNode2Exist) || (isNode1Exist && !isNode2Exist)) &&
        nodeNum + 1 > NODE_LIMIT
      ) {
        console.log("deleteOneNode");
        //노드 2개 삭제 후 관련 RELATION 모두 삭제
        let findDeleteNode = await MyNode.find({
          options: {
            sort: [
              {
                createdAt: "ASC",
              },
            ],
            limit: 1,
          },
        });
        findDeleteNode.map(async (node) => {
          await MyNode.delete({
            where: {
              type: node.type,
              value: node.value,
            },
          });
        });
      }

      let relExist = await MyNode.find({
        where: {
          value: E1_value,
          type: E1_type,
          relOutConnection_SINGLE: {
            node: {
              value: E2_value,
              type: E2_type,
            },
          },
        },
      });
      relExist = relExist.length > 0;

      if (
        !(isNode1Exist && isNode2Exist && relExist) &&
        relationNum + 1 > RELATION_LIMIT
      ) {
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

        let a = await MyNode.find({
          selectionSet: selectionSet2,
          where: {
            relOutAggregate: {
              count_GT: 0,
            },
          },
          options: {
            sort: [
              {
                createdAt: "ASC",
              },
            ],
            limit: 1,
          },
        });
        let nodeType1 = a[0].type;
        let nodeValue1 = a[0].value;
        let nodeType2 = a[0].relOut[a[0].relOut.length - 1].type;
        let nodeValue2 = a[0].relOut[a[0].relOut.length - 1].value;
        console.log(nodeType1, nodeValue1, nodeType2, nodeValue2);
        await MyNode.update({
          where: {
            type: nodeType1,
            value: nodeValue1,
          },
          update: {
            relOut: [
              {
                disconnect: [
                  {
                    where: {
                      node: {
                        type: nodeType2,
                        value: nodeValue2,
                      },
                    },
                  },
                ],
              },
            ],
          },
        });
        console.log(relExist);
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
                  where: {
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
                  where: {
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
            type: E1_type,
          },
          update: {
            relOut: {
              connect: {
                where: {
                  node: { value: E2_value, type: E2_type },
                },
                edge: {
                  name: REL,
                },
              },
            },
          },
        });
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
    context: async ({ req }) => {
      return { req };
    },
  });

  server.listen().then(async ({ url }) => {
    console.log(`🚀 Server ready at ${url}`);
    let isInfoExist = await Info.find({
      where: { id: 0 },
    });
    if (isInfoExist.length === 0) {
      await Info.create({
        input: [
          {
            id: 0,
          },
        ],
      });
    }
  });
});
