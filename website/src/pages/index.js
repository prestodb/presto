import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import useBaseUrl from '@docusaurus/useBaseUrl';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner, 'jumbotron')}>
      <div className="container">

      <div className="row">

        <div className="col col--3 col--offset-1">
            <img className={styles.heroLogo}
                 src={useBaseUrl("img/velox-logo-white.png")} />
        </div>

        <div className="col col--5 col--offset-1">
          <h1 className={clsx("hero__title", styles.clHeroTitle)}>{siteConfig.title}</h1>
          <p className={clsx("hero__subtitle", styles.clHeroTagLine)}>
            {`${siteConfig.tagline}`}
          </p>

          <div className="button-wrapper">
            <Link
              className={clsx(
                'button button--success button--lg',
                styles.getStarted,
              )}
              to={ useBaseUrl('docs/') }>
              Learn More
            </Link>
          </div>
        </div>
      </div>
    </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Description will go into a meta tag in <head />">
      <HomepageHeader />
      <main>
          <section className={styles.features}>
              <div className="container">
                  <div className="row">
                      <div className={clsx('col')}>
                          <img src={useBaseUrl("img/stack_transform.png")}/>
                      </div>
                  </div>
              </div>
          </section>
      </main>
    </Layout>
  );
}
