import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import VeloxConBanner from '@site/src/components/VeloxConBanner';
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
              to={ "https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md" }>
              Join Us
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
          <VeloxConBanner />
          <section className={styles.features}>
              <div className="container">
                  <div className="row">
                      <div className={clsx('col')}>
                          <center>
                          <iframe width="70%" height="504" src="https://www.youtube.com/embed/T9NMWN7vuSc"
                                  title="YouTube video player" frameBorder="0"
                                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                                  allowFullScreen></iframe>
                          </center>
                      </div>
                  </div>
              </div>
          </section>
          <HomepageFeatures />
      </main>
    </Layout>
  );
}
