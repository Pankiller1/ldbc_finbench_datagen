/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.generation.{DatagenContext, DatagenParams}
import ldbc.finbench.datagen.generation.events._
import ldbc.finbench.datagen.generation.events.AccountActivitiesEvent.WithdrawCard
import ldbc.finbench.datagen.util.Logging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.SortedMap

class ActivityGenerator(config: DatagenConfiguration)(implicit spark: SparkSession)
    extends Serializable
    with Logging {

  val blockSize: Int = DatagenParams.blockSize
  val sampleRandom = new scala.util.Random(DatagenParams.defaultSeed)
  val accountGenerator = new AccountGenerator()
  val loanGenerator = new LoanGenerator()

  // including account, loan, guarantee
  def personActivitiesEvent(personRDD: RDD[Person]): RDD[Person] = {
    val personActivitiesEvent = new PersonActivitiesEvent
    val blocks = personRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }

    val personWithAccountsLoansGuarantees = blocks
      .combineByKeyWithClassTag(
        personByRank => SortedMap(personByRank),
        (map: SortedMap[Long, Person], personByRank) => map + personByRank,
        (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
      )
      .mapPartitions(groups => {
        DatagenContext.initialize(config)
        groups.flatMap { case (block, persons) =>
          personActivitiesEvent
            .personActivities(
              persons.values.toList.asJava,
              accountGenerator,
              loanGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })

    personWithAccountsLoansGuarantees
  }

  def companyActivitiesEvent(companyRDD: RDD[Company]): RDD[Company] = {
    val companyActivitiesEvent = new CompanyActivitiesEvent
    val blocks = companyRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }

    val companyWithAccountsLoansGuarantees = blocks
      .combineByKeyWithClassTag(
        companyByRank => SortedMap(companyByRank),
        (map: SortedMap[Long, Company], companyByRank) => map + companyByRank,
        (a: SortedMap[Long, Company], b: SortedMap[Long, Company]) => a ++ b
      )
      .mapPartitions(groups => {
        DatagenContext.initialize(config)
        groups.flatMap { case (block, companies) =>
          companyActivitiesEvent
            .companyActivities(
              companies.values.toList.asJava,
              accountGenerator,
              loanGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })

    companyWithAccountsLoansGuarantees
  }

  def investEvent(
      personRDD: RDD[Person],
      companyRDD: RDD[Company]
  ): RDD[Company] = {
    // Lightweight broadcast: only id and creationDate instead of full Person objects (with nested accounts, loans, etc.)
    val personInfos = spark.sparkContext.broadcast(
      personRDD.map(p => new InvestorInfo(p.getPersonId, p.getCreationDate)).collect()
    )
    // Lightweight broadcast: only id and creationDate instead of full Company objects
    val companyInfos = spark.sparkContext.broadcast(
      companyRDD.map(c => new InvestorInfo(c.getCompanyId, c.getCreationDate)).collect()
    )

    val personInvestEvent = new PersonInvestEvent()
    val companyInvestEvent = new CompanyInvestEvent()

    companyRDD
      .sample(
        withReplacement = false,
        DatagenParams.companyInvestedFraction,
        sampleRandom.nextLong()
      )
      .mapPartitionsWithIndex { (index, targets) =>
        DatagenContext.initialize(config)
        personInvestEvent.resetState(index)
        personInvestEvent
          .personInvestPartition(personInfos.value, targets.toList.asJava)
          .iterator()
          .asScala
      }
      .mapPartitionsWithIndex { (index, targets) =>
        DatagenContext.initialize(config)
        companyInvestEvent.resetState(index)
        companyInvestEvent
          .companyInvestPartition(
            companyInfos.value,
            targets.toList.asJava
          )
          .iterator()
          .asScala
      }
      .map(_.scaleInvestmentRatios())
  }

  def mediumActivitesEvent(
      mediumRDD: RDD[Medium],
      accountRDD: RDD[Account]
  ): RDD[Medium] = {
    // Lightweight broadcast: only fields needed by SignIn edge creation instead of full Account objects
    val accountSampleList = spark.sparkContext.broadcast(
      accountRDD
        .sample(
          withReplacement = false,
          DatagenParams.accountSignedInFraction,
          sampleRandom.nextLong()
        )
        .map(a => new SignInTargetInfo(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isExplicitlyDeleted))
        .collect()
    )

    val signInEvent = new SignInEvent
    mediumRDD.mapPartitionsWithIndex((index, mediums) => {
      DatagenContext.initialize(config)
      signInEvent
        .signIn(
          mediums.toList.asJava,
          accountSampleList.value,
          index
        )
        .iterator()
        .asScala
    })
  }

  def accountActivitiesEvent(accountRDD: RDD[Account]): RDD[Account] = {
    // Derive cards RDD from the same accountRDD, keeping the same partitioning.
    // Each partition's cards are co-located with its accounts via zipPartitions,
    // avoiding the broadcast-collect bottleneck at large scale.
    val cardsRDD: RDD[WithdrawCard] = accountRDD
      .filter(_.getType == "debit card")
      .map(a => new WithdrawCard(a.getAccountId, a.getType, a.getCreationDate, a.getDeletionDate, a.isExplicitlyDeleted))

    val accountActivitiesEvent = new AccountActivitiesEvent
    accountRDD.zipPartitions(cardsRDD) { (accountsIter, cardsIter) =>
      DatagenContext.initialize(config)
      val partitionId = TaskContext.getPartitionId()
      accountActivitiesEvent
        .accountActivities(
          accountsIter.toArray,
          cardsIter.toArray,
          partitionId
        )
        .iterator()
        .asScala
    }
  }

  def afterLoanSubEvents(
      loanRDD: RDD[Loan],
      accountRDD: RDD[Account]
  ): (RDD[Loan]) = {
    val sampledAccounts = spark.sparkContext.broadcast(
      accountRDD
        .sample(
          withReplacement = false,
          DatagenParams.loanInvolvedAccountsFraction,
          sampleRandom.nextLong()
        )
        .collect()
    )

    loanRDD.mapPartitionsWithIndex((index, loans) => {
      DatagenContext.initialize(config)
      val loanSubEvents = new LoanActivitiesEvents
      loanSubEvents
        .afterLoanApplied(
          loans.toList.asJava,
          sampledAccounts.value,
          index
        )
        .iterator()
        .asScala
    })
  }
}
