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
      type: String!
      url: [String]
      main: String
      sub: String
      title: String
      uploadTime: String
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
      url: [String]
      main: String
      sub: String
      title: String
      uploadTime: String
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
        E1_url: String
        E1_main: String
        E1_sub: String
        E1_title: String
        E1_uploadTime: String
        E2_value: String!
        E2_type: String!
        E2_url: String
        E2_main: String
        E2_sub: String
        E2_title: String
        E2_uploadTime: String
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
  'no_relation':"?????? ??????",
  'org:dissolved':'?????? ????????? ????????? ??????',
  'org:founded':'?????? ????????? ????????? ??????',
  'org:place_of_headquarters':'?????? ????????? ????????? ????????? ??????',
  'org:alternate_names':'?????? ????????? ?????? ??????',
  'org:member_of':'?????? ????????? ??????',
  'org:members':'?????? ????????? ?????? ????????????',
  'org:political/religious_affiliation':'?????? ????????? ????????? ??????/?????? ??????',
  'org:product':'?????? ???????????? ????????? ??????',
  'org:founded_by':'?????? ????????? ?????????',
  'org:top_members/employees':'?????? ????????? ?????? ?????????',
  'org:number_of_employees/members':'?????? ????????? ????????? ????????? ???',
  'per:date_of_birth':'?????? ????????? ????????? ??????',
  'per:date_of_death':'?????? ????????? ?????? ??????',
  'per:place_of_birth':'?????? ????????? ????????? ??????',
  'per:place_of_death':'?????? ????????? ?????? ??????',
  'per:place_of_residence':'?????? ????????? ???????????? ??????',
  'per:origin':'?????? ????????? ??????',
  'per:employee_of':'?????? ????????? ???????????? ??????',
  'per:schools_attended':'?????? ????????? ?????? ??????',
  'per:alternate_names':'?????? ????????? ?????? ??????',
  'per:parents':'?????? ????????? ?????????',
  'per:siblings':'?????? ????????? ??????',
  'per:spouse':'?????? ????????? ?????????',
  'per:other_family':'?????? ????????? ??????, ?????????, ????????? ????????? ????????? ??????',
  'per:colleagues':'?????? ????????? ????????????',
  'per:product':'?????? ????????? ????????? ??????',
  'per:religion':'?????? ????????? ??????',
  'per:title':'?????? ????????? ??????'
}

const resolvers = {
  Query: {
    search: async (_, { limit, words }, ___) => {
      const selectionSet = `
        {
          value
          type
          url
          main
          sub
          title
          uploadTime
          relOutConnection {
            edges {
              name
            }
          }
          relOut {
            value
            type
            url
            main
            sub
            title
            uploadTime
          }
          relInConnection {
            edges {
              name
            }
          }
          relIn {
            value
            type
            url
            main
            sub
            title
            uploadTime
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
          nodes.push({ id: source.value, type: source.type, url: source.url, main: source.main, sub: source.sub, title: source.title, uploadTime: source.uploadTime });
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
            nodes.push({ id: source.value, type: source.type, url: source.url, main: source.main, sub: source.sub, title: source.title, uploadTime: source.uploadTime });
            source.relOut.map((target, index) => {
              //if (nodes.length >= limit) return;
              nodes.push({ id: target.value, type: target.type, url: target.url, main: target.main, sub: target.sub, title: target.title, uploadTime: target.uploadTime });
              links.push({
                source: source.value,
                target: target.value,
                label: source.relOutConnection.edges[index].name[0],
              });
            });
            source.relIn.map((target, index) => {
              //if (nodes.length >= limit) return;
              nodes.push({ id: target.value, type: target.type, url: target.url, main: target.main, sub: target.sub, title: target.title, uploadTime: target.uploadTime });
              links.push({
                source: target.value,
                target: source.value,
                label: source.relInConnection.edges[index].name[0],})
            });
          });
        }
      }
      // ????????????
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
      { E1_value, E1_type, E1_url, E1_main, E1_sub, E1_title, E1_uploadTime, E2_value, E2_type, E2_url, E2_main, E2_sub, E2_title, E2_uploadTime, REL },
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
        //?????? ??????
        desc = nodes[0];
      });

      let nodeNum = desc.countNode; //?????? ??????
      let relationNum = desc.countRelation;
      console.log(nodeNum, relationNum);

      //Node ???????????? ????????????
      let node1 = await MyNode.find({
        where: { value: E1_value },
      });
      let node2 = await MyNode.find({
        where: { value: E2_value },
      });
      isNode1Exist = node1.length >= 1;
      isNode2Exist = node2.length >= 1;

      if (!isNode1Exist && !isNode2Exist && nodeNum + 2 > NODE_LIMIT) {
        console.log("deleteTwoNode");
        //?????? 2??? ?????? ??? ?????? RELATION ?????? ??????
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
        //?????? 2??? ?????? ??? ?????? RELATION ?????? ??????
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

      //?????? ???????????? ?????????
      if (!isNode1Exist && !isNode2Exist) {
        MyNode.create({
          input: [
            {
              value: E1_value,
              type: E1_type,
              main: E1_main,
              sub: E1_sub,
              url: [E1_url],
              title: E1_title,
              uploadTime: E1_uploadTime,
              relOut: {
                create: {
                  node: { value: E2_value, type: E2_type, main: E2_main, sub: E2_sub, url: [E2_url], title: E2_title, uploadTime: E2_uploadTime },
                  edge: {
                    name: REL,
                  },
                },
              },
            },
          ],
        });
      }
      //node2??? ????????????
      else if (!isNode1Exist && isNode2Exist) {
        var newUrl = node2[0].url
        newUrl.push(E1_url)
        const set = new Set(newUrl);
        newUrl = [...set];
        await MyNode.update({
          where: { value: E2_value },
          update: { url: newUrl }
        })
        MyNode.create({
          input: [
            {
              value: E1_value,
              type: E1_type,
              main: E1_main,
              sub: E1_sub,
              url: [E1_url],
              title: E1_title,
              uploadTime: E1_uploadTime,
              relOut: {
                connect: {
                  where: {
                    node: { 
                      value: E2_value, 
                      //type: E2_type 
                    },
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
      //node1??? ????????????
      else if (isNode1Exist && !isNode2Exist) {
        var newUrl = node1[0].url
        newUrl.push(E1_url)
        const set = new Set(newUrl);
        newUrl = [...set];
        await MyNode.update({
          where: { value: E1_value },
          update: { url: newUrl }
        })
        MyNode.create({
          input: [
            {
              value: E2_value, 
              type: E2_type, 
              main: E2_main, 
              sub: E2_sub, 
              url: [E2_url], 
              title: E2_title, 
              uploadTime: E2_uploadTime,
              relIn: {
                connect: {
                  where: {
                    node: { 
                      value: E1_value, 
                      //type: E1_type 
                    },
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
      //?????? ????????????
      else {
        var newUrl1 = node1[0].url
        newUrl1.push(E1_url)
        const set1 = new Set(newUrl1);
        newUrl1 = [...set1];
        await MyNode.update({
          where: { value: E1_value },
          update: { url: newUrl1 }
        })
        var newUrl2 = node2[0].url
        newUrl2.push(E2_url)
        const set2 = new Set(newUrl2);
        newUrl2 = [...set2];
        await MyNode.update({
          where: { value: E2_value },
          update: { url: newUrl2 }
        })
        MyNode.update({
          where: {
            value: E1_value,
            //type: E1_type,
          },
          update: {
            relOut: {
              connect: {
                where: {
                  node: { 
                    value: E2_value, 
                    //type: E2_type 
                  },
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
    console.log(`???? Server ready at ${url}`);
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
